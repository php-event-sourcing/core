<?php

declare(strict_types=1);

namespace DbalEs\Postgres;

use DbalEs\Dbal\Connection;
use DbalEs\Event;
use DbalEs\EventStore;
use DbalEs\EventStreamId;
use DbalEs\PersistedEvent;
use DbalEs\Subscription\SubscriptionLoader;
use DbalEs\Subscription\SubscriptionPosition;
use DbalEs\Subscription\SubscriptionQuery;
use Psr\EventDispatcher\EventDispatcherInterface;

class PostgresEventStore implements EventStore, SubscriptionLoader
{
    public function __construct(
        private Connection $connection,
        private ?PostgresProjectionManager $projectionManager = null,
    ) {
    }

    /**
     * @inheritDoc
     */
    public function append(EventStreamId $eventStreamId, array $events): array
    {
        $streamVersionStatement = $this->connection->prepare('SELECT version FROM public.es_stream WHERE stream_id = ? FOR UPDATE');
        $streamVersionStatement->execute([(string) $eventStreamId->streamId]);
        $actualStreamVersion = $streamVersionStatement->fetchColumn() ?: null;

        if ($eventStreamId->version && $actualStreamVersion !== $eventStreamId->version) {
            throw new \RuntimeException('Concurrency error. Expected version ' . $eventStreamId->version . ' but got ' . $actualStreamVersion);
        }
        $version = $actualStreamVersion ?? 0;
        $statement = $this->connection->prepare(<<<SQL
INSERT INTO public.es_event (stream_id, version, event_type, json_data)
VALUES (?, ?, ?, ?)
RETURNING id, transaction_id
SQL);
        $persistedEvents = [];
        foreach ($events as $event) {
            $statement->execute([
                $eventStreamId->streamId,
                $version++,
                $event->type,
                json_encode($event->data),
            ]);
            $row = $statement->fetch();
            $persistedEvents[] = new PersistedEvent(
                new SubscriptionPosition((int) $row['transaction_id'], (int) $row['id']),
                $event,
                new EventStreamId($eventStreamId->streamId, $version),
            );
        }
        if ($actualStreamVersion === null) {
            $this->connection->prepare('INSERT INTO public.es_stream (stream_id, version) VALUES (?, ?)')->execute([$eventStreamId->streamId, $version]);
        } else {
            $this->connection->prepare('UPDATE public.es_stream SET version = ? WHERE stream_id = ?')->execute([$version, $eventStreamId->streamId]);
        }

        $this->projectionManager?->run($persistedEvents);

        return $persistedEvents;
    }

    /**
     * @inheritDoc
     */
    public function load(EventStreamId $eventStreamId): iterable
    {
        $statement = $this->connection->prepare('SELECT id, transaction_id, version, event_type, json_data FROM public.es_event WHERE stream_id = ? ORDER BY id');
        $statement->execute([$eventStreamId->streamId]);

        $events = [];
        while ($row = $statement->fetch()) {
            $events[] = new PersistedEvent(
                new SubscriptionPosition((int) $row['transaction_id'], (int) $row['id']),
                new Event($row['event_type'], $row['json_data']),
                new EventStreamId($eventStreamId->streamId, (int) $row['version']),
            );
        }

        return $events;
    }

    /**
     * @return iterable<PersistedEvent>
     */
    public function read(SubscriptionQuery $query): iterable
    {
        $whereParts = [];
        $params = [];
        if ($query->streamIds) {
            $whereParts[] = 'e.STREAM_ID IN (' . implode(', ', array_fill(0, count($query->streamIds), '?')) . ')';
            $params = array_merge($params, $query->streamIds);
        }
        if ($query->from) {
            $whereParts[] = '(e.TRANSACTION_ID, e.ID) > (?, ?)';
            $params[] = $query->from->transactionId;
            $params[] = $query->from->sequenceNumber;
        }
        if ($query->allowGaps === false) {
            $whereParts[] = 'e.TRANSACTION_ID < pg_snapshot_xmin(pg_current_snapshot())';
        }

        $where = $whereParts ? 'WHERE ' . implode(' AND ', $whereParts) : '';
        $limit = $query->limit ? "LIMIT {$query->limit}" : '';

        $query = <<<SQL
SELECT e.id, e.transaction_id, e.stream_id, e.version, e.event_type, e.json_data FROM es_event e 
{$where}
ORDER BY e.TRANSACTION_ID, e.ID
{$limit}
SQL;

        $statement = $this->connection->prepare($query);
        $statement->execute($params);

        while ($row = $statement->fetch()) {
            yield new PersistedEvent(
                new SubscriptionPosition((int) $row['transaction_id'], (int) $row['id']),
                new Event($row['event_type'], $row['json_data']),
                new EventStreamId($row['stream_id'], (int) $row['version']),
            );
        }
    }

    public function connection(): Connection
    {
        return $this->connection;
    }

    public function projectionManager(): ?PostgresProjectionManager
    {
        return $this->projectionManager;
    }
}