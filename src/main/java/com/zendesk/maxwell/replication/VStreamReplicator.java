package com.zendesk.maxwell.replication;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.zendesk.maxwell.MaxwellVitessConfig;
import com.zendesk.maxwell.errors.DuplicateProcessException;
import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.monitoring.Metrics;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.replication.vitess.ReplicationMessageColumn;
import com.zendesk.maxwell.replication.vitess.Vgtid;
import com.zendesk.maxwell.replication.vitess.VitessSchema;
import com.zendesk.maxwell.replication.vitess.VitessTable;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMapBuffer;
import com.zendesk.maxwell.schema.MysqlPositionStore;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaStoreException;
import com.zendesk.maxwell.schema.VitessPositionStore;
import com.zendesk.maxwell.util.RunLoopProcess;

import binlogdata.Binlogdata.FieldEvent;
import binlogdata.Binlogdata.RowChange;
import binlogdata.Binlogdata.RowEvent;
import binlogdata.Binlogdata.VEvent;
import binlogdata.Binlogdata.VEventType;
import binlogdata.Binlogdata.VGtid;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import io.vitess.proto.Vtgate.VStreamFlags;
import io.vitess.proto.Vtgate.VStreamRequest;
import io.vitess.proto.grpc.VitessGrpc;
import io.vitess.proto.Topodata;

public class VStreamReplicator extends RunLoopProcess implements Replicator {
	private static final Logger LOGGER = LoggerFactory.getLogger(VStreamReplicator.class);

	private static final int GRPC_MAX_INBOUND_MESSAGE_SIZE = 4 * 1024 * 1024;
	public static final int KEEPALIVE_INTERVAL_SECONDS = 60;
	public static final int HEARTBEAT_INTERVAL_SECONDS = 30;

	private static final long MAX_TX_ELEMENTS = 10000;

	private static final String INSERT_TYPE = "INSERT";
	private static final String UPDATE_TYPE = "UPDATE";
	private static final String DELETE_TYPE = "DELETE";

	private final MaxwellVitessConfig vitessConfig;
	private final AbstractProducer producer;
	private final VitessPosition initPosition;
	private final VitessPositionStore positionStore;
	private RowMapBuffer rowBuffer;
	private final float bufferMemoryUsage;

	private ManagedChannel channel;
	private VStreamObserver responseObserver;
	private boolean replicatorStarted = false;

	private final LinkedBlockingDeque<VEvent> queue;
	private final VitessSchema vitessSchema = new VitessSchema();
	private final Filter filter;

	private final Counter rowCounter;
	private final Meter rowMeter;
	private final Histogram transactionRowCount;
	private final Histogram transactionExecutionTime;

	public VStreamReplicator(
			MaxwellVitessConfig vitessConfig,
			AbstractProducer producer,
			MysqlPositionStore positionStore,
			Position initPosition,
			Metrics metrics,
			Filter filter,
			Float bufferMemoryUsage,
			int binlogEventQueueSize) {
		this.vitessConfig = vitessConfig;
		this.producer = producer;
		this.initPosition = (VitessPosition) initPosition;
		this.positionStore = (VitessPositionStore) positionStore;
		this.queue = new LinkedBlockingDeque<>(binlogEventQueueSize);
		this.filter = filter;
		this.bufferMemoryUsage = bufferMemoryUsage;

		/* setup metrics */
		MetricRegistry mr = metrics.getRegistry();
		rowCounter = mr.counter(metrics.metricName("row", "count"));
		rowMeter = mr.meter(metrics.metricName("row", "meter"));
		transactionRowCount = mr.histogram(metrics.metricName("transaction", "row_count"));
		transactionExecutionTime = mr.histogram(metrics.metricName("transaction", "execution_time"));
	}

	public void startReplicator() throws Exception {
		LOGGER.info(
				"Starting VStreamReplicator, connecting to Vtgate at {}:{}",
				vitessConfig.vtgateHost, vitessConfig.vtgatePort);

		this.channel = newChannel(
				vitessConfig.vtgateHost,
				vitessConfig.vtgatePort,
				GRPC_MAX_INBOUND_MESSAGE_SIZE);
		VitessGrpc.VitessStub stub = VitessGrpc.newStub(channel);

		VStreamFlags vStreamFlags = VStreamFlags.newBuilder()
				.setStopOnReshard(true)
				.setHeartbeatInterval(HEARTBEAT_INTERVAL_SECONDS)
				.build();

		VStreamRequest vstreamRequest = VStreamRequest.newBuilder()
				.setVgtid(initialVgtid())
				.setTabletType(Topodata.TabletType.MASTER)
				.setFlags(vStreamFlags)
				.build();

		this.responseObserver = new VStreamObserver(queue);

		stub.vStream(vstreamRequest, responseObserver);

		LOGGER.info("Started VStream");

		this.replicatorStarted = true;
	}

	@Override
	protected void beforeStop() throws Exception {
		responseObserver.stop();

		channel.shutdown();
		channel.awaitTermination(500, TimeUnit.MILLISECONDS);
		channel.shutdownNow();
	}

	/**
	 * get a single row from the replicator and pass it to the producer or
	 * bootstrapper.
	 *
	 * This is the top-level function in the run-loop.
	 */
	@Override
	public void work() throws Exception {
		RowMap row = null;
		try {
			row = getRow();
		} catch (InterruptedException e) {
			LOGGER.debug("Interrupted while waiting for row");
		}

		if (row == null) {
			checkIfVstreamIsAlive();
			return;
		}

		rowCounter.inc();
		rowMeter.mark();

		// if ( scripting != null && !isMaxwellRow(row))
		// scripting.invoke(row);

		processRow(row);
	}

	private void checkIfVstreamIsAlive() {
		if (!replicatorStarted) return;

		Exception lastException = responseObserver.getLastException();
		if (lastException != null) {
			LOGGER.error("VStream is dead, stopping...");
			throw new RuntimeException(lastException);
		}
	}

	public RowMap getRow() throws Exception {
		if (!replicatorStarted) {
			LOGGER.warn("replicator was not started, calling startReplicator()...");
			startReplicator();
		}

		while (true) {
			if (rowBuffer != null && !rowBuffer.isEmpty()) {
				return rowBuffer.removeFirst();
			}

			VEvent event = pollEvent();
			if (event == null) {
				return null;
			}

			if (event.getType() == VEventType.BEGIN) {
				rowBuffer = getTransactionRows(event);
			} else {
				processServiceEvent(event);
			}
		}
	}

	private VGtid initialVgtid() {
		Vgtid initialVgtid;
		if (initPosition != null) {
			initialVgtid = initPosition.getVgtid();
		} else {
			Vgtid.ShardGtid shardGtid = new Vgtid.ShardGtid(vitessConfig.keyspace, vitessConfig.shard, "current");
			initialVgtid = Vgtid.of(List.of(shardGtid));
		}

		LOGGER.debug("Setting the initial vgtid for the stream to {}", initialVgtid);
		return initialVgtid.getRawVgtid();
	}

	private void processRow(RowMap row) throws Exception {
		producer.push(row);
	}

	private VEvent pollEvent() throws InterruptedException {
		return queue.poll(100, TimeUnit.MILLISECONDS);
	}

	private void processFieldEvent(VEvent event) {
		FieldEvent fieldEvent = event.getFieldEvent();
		LOGGER.debug("Received field event: {}", fieldEvent);
		vitessSchema.processFieldEvent(fieldEvent);
	}

	private VitessPosition processVGtidEvent(VEvent event) {
		LOGGER.debug("Received GTID event: {}", event);
		return new VitessPosition(Vgtid.of(event.getVgtid()));
	}

	/**
	 * Get a batch of rows for the current transaction.
	 *
	 * We assume the replicator has just processed a "BEGIN" event, and now
	 * we're inside a transaction. We'll process all rows inside that transaction
	 * and turn them into RowMap objects.
	 *
	 * We do this because we want to attach all rows within
	 * the transaction the same transaction id value (xid, which we generate
	 * ourselves since VStream
	 * does not expose underlying transaction ids to the consumer).
	 *
	 * @return A RowMapBuffer of rows; either in-memory or on disk.
	 */
	private RowMapBuffer getTransactionRows(VEvent beginEvent) throws Exception {
		RowMapBuffer buffer = new RowMapBuffer(MAX_TX_ELEMENTS, this.bufferMemoryUsage);

		// Since transactions in VStream do not have an XID value, we generate one
		long xid = System.currentTimeMillis() * 1000 + Math.abs(beginEvent.hashCode()) % 1000;
		LOGGER.debug("Generated transaction id: {}", xid);
		buffer.setXid(xid);

		// Since specific VStream events do not provide the VGTID, we capture it from
		// VGTID events present in each transaction.
		VitessPosition latestPosition = null;

		while (true) {
			final VEvent event = pollEvent();
			if (event == null) {
				continue;
			}

			final VEventType eventType = event.getType();

			if (eventType == VEventType.VGTID) {
				latestPosition = processVGtidEvent(event);
				continue;
			}

			if (eventType == VEventType.COMMIT) {
				LOGGER.debug("Received COMMIT event");
				if (!buffer.isEmpty()) {
					// Set TX flag and the position on the last row in the transaction
					RowMap lastEvent = buffer.getLast();
					if (latestPosition != null) {
						lastEvent.setNextPosition(latestPosition);
					} else {
						throw new RuntimeException("VGTID is null for transaction");
					}
					lastEvent.setTXCommit();

					long timeSpent = buffer.getLast().getTimestampMillis() - beginEvent.getTimestamp();
					transactionExecutionTime.update(timeSpent);
					transactionRowCount.update(buffer.size());
				}
				return buffer;
			}

			if (eventType == VEventType.ROW) {
				List<RowMap> eventRows = rowEventToMaps(event, xid);
				for (RowMap r : eventRows) {
					// if (shouldOutputRowMap(table.getDatabase(), table.getName(), r, filter)) {
					buffer.add(r);
				}
				continue;
			}

			// All other events are service events delivering the schema, GTID values, etc.
			processServiceEvent(event);
		}
	}

	private void processServiceEvent(VEvent event) throws SQLException, DuplicateProcessException {
		final VEventType eventType = event.getType();
		switch (eventType) {
			case FIELD:
				processFieldEvent(event);
				break;

			case HEARTBEAT:
				LOGGER.debug("Received heartbeat from vtgate: {}", event);
				break;

			case VGTID:
				// Use an initial VGTID event received after connecting to vtgate as for setting
				// the initial position of the stream.
				if (initPosition == null) {
					VitessPosition position = processVGtidEvent(event);
					LOGGER.info("Current VGTID event received, using it for initial positioning at {}", position);
					positionStore.set(position);
				} else {
					LOGGER.warn("Ignoring a standalone VGTID event, we already have an initial position: {}", event);
				}
				break;

			case ROW:
			case BEGIN:
			case COMMIT:
				LOGGER.error("Unexpected event outside of a transaction, skipping: {}", event);
				break;

			default:
				LOGGER.debug("Unsupported service event: {}", event);
				break;
		}
	}

	private List<RowMap> rowEventToMaps(VEvent event, long xid) {
		Long timestampMillis = event.getCurrentTime();
		RowEvent rowEvent = event.getRowEvent();
		String qualifiedTableName = rowEvent.getTableName();

		List<RowMap> rowMaps = new ArrayList<>(rowEvent.getRowChangesCount());
		for (RowChange rowChange : rowEvent.getRowChangesList()) {
			String changeType = rowChangeToMaxwellType(rowChange);
			final VitessTable table = resolveTable(qualifiedTableName);
			if (!filter.includes(table.getSchemaName(), table.getTableName())) {
				LOGGER.debug("Filtering out event for table {}.{}", table.getSchemaName(), table.getTableName());
				continue;
			}
			List<ReplicationMessageColumn> columns = table.resolveColumns(rowChange);

			RowMap rowMap = new RowMap(
					changeType,
					table.getSchemaName(),
					table.getTableName(),
					timestampMillis,
					table.getPkColumns(),
					null, null, null);

			rowMap.setXid(xid);

			for (ReplicationMessageColumn column : columns) {
				rowMap.putData(column.getName(), column.getValue());
			}

			rowMaps.add(rowMap);
		}

		return rowMaps;
	}

	private String rowChangeToMaxwellType(RowChange change) {
		if (change.hasAfter() && !change.hasBefore()) {
			return INSERT_TYPE;
		}
		if (change.hasAfter() && change.hasBefore()) {
			return UPDATE_TYPE;
		}
		if (!change.hasAfter() && change.hasBefore()) {
			return DELETE_TYPE;
		}

		throw new RuntimeException("Invalid row change: " + change);
	}

	private VitessTable resolveTable(String qualifiedTableName) {
		VitessTable table = vitessSchema.getTable(qualifiedTableName);
		if (table == null) {
			throw new RuntimeException("Table not found in the schema: " + qualifiedTableName);
		}
		return table;
	}

	@Override
	public Long getLastHeartbeatRead() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Schema getSchema() throws SchemaStoreException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long getSchemaId() throws SchemaStoreException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void stopAtHeartbeat(long heartbeat) {
		// TODO Auto-generated method stub
	}

	private static ManagedChannel newChannel(String vtgateHost, int vtgatePort, int maxInboundMessageSize) {
		return ManagedChannelBuilder
				.forAddress(vtgateHost, vtgatePort)
				.usePlaintext()
				.maxInboundMessageSize(maxInboundMessageSize)
				.keepAliveTime(KEEPALIVE_INTERVAL_SECONDS, TimeUnit.SECONDS)
				.build();
	}
}
