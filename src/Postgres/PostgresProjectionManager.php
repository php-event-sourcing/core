<?php

declare(strict_types=1);

namespace DbalEs\Postgres;

use DbalEs\Dbal\Connection;
use DbalEs\PersistedEvent;
use DbalEs\Projection\PollingProjectionManager;
use DbalEs\Projection\Projector;
use DbalEs\Projection\ProjectorWithSetup;
use DbalEs\Subscription\SubscriptionLoader;
use DbalEs\Subscription\SubscriptionPosition;
use DbalEs\Subscription\SubscriptionQuery;

class PostgresProjectionManager implements PollingProjectionManager
{
    /**
     * @param array<string, Projector> $projectors
     */
    public function __construct(
        private Connection $connection,
        private array $projectors,
        private bool $ignoreUnknownProjectors = false
    ) {
    }

    /**
     * @param array<PersistedEvent> $events
     */
    public function run(array $events): void
    {
        $statement = $this->connection->prepare(<<<SQL
SELECT projector FROM es_projection
WHERE state = 'inline' AND (
    after_transaction_id IS NULL 
        OR 
    pg_current_xact_id_if_assigned() IS NULL
        OR 
    after_transaction_id < pg_current_xact_id_if_assigned())
ORDER BY projector
SQL);
        $statement->execute();

        while ($projection = $statement->fetch()) {
            $projector = $this->projectors[$projection['projector']] ?? null;
            if (!$projector) {
                if ($this->ignoreUnknownProjectors) {
                    continue;
                }
                throw new \RuntimeException(\sprintf('Unknown projector "%s"', $projection['projector']));
            }
            foreach ($events as $event) {
                $projector->project($event);
            }
        }
    }

    public function addProjection(string $projectorName, string $state = "catchup"): void
    {
        $projector = $this->getProjector($projectorName);

        $this->connection->prepare(<<<SQL
            INSERT INTO es_projection (projector, state)
            VALUES (?, ?)
            SQL)
            ->execute([$projectorName, $state]);

        if ($projector instanceof ProjectorWithSetup) {
            $projector->setUp();
        }
    }

    public function removeProjection(string $projectorName): void
    {
        $this->connection->prepare(<<<SQL
            DELETE FROM es_projection
            WHERE projector = ?
            SQL)
            ->execute([$projectorName]);

        try {
            $projector = $this->getProjector($projectorName);
            if ($projector instanceof ProjectorWithSetup) {
                $projector->tearDown();
            }
        } catch (\RuntimeException) {
            // ignore
        }
    }

    public function catchupProjection(string $projectorName, SubscriptionLoader $subscriptionLoader, int $missingEventsMaxLoops = 100): void
    {
        $this->connection->beginTransaction();
        try {
            $statement = $this->connection->prepare(<<<SQL
                SELECT projector, state, metadata FROM es_projection
                WHERE projector = ?
                FOR UPDATE
                SQL);
            $statement->execute([$projectorName]);
            $projection = $statement->fetch();
            if (!$projection) {
                throw new \RuntimeException('Projection not found');
            }
            if ($projection['state'] !== 'catchup') {
                throw new \RuntimeException('Projection is not in catchup state');
            }
            $projector = $this->getProjector($projectorName);
            if ($projection['metadata']) {
                $metadata = json_decode($projection['metadata'], true);
                $transactionId = (int) $metadata['transaction_id'] ?? throw new \RuntimeException('Missing transaction_id in metadata');
                $eventId = (int) $metadata['event_id'] ?? throw new \RuntimeException('Missing event_id in metadata');
                $position = new SubscriptionPosition($transactionId, $eventId);
            } else {
                $position = null;
            }

            $lastPosition = $position;
            foreach ($subscriptionLoader->read(new SubscriptionQuery(from: $position)) as $event) {
                $projector->project($event);
                $lastPosition = $event->eventId;
            }

            $lastTransactionIdStatement = $this->connection->prepare(<<<SQL
                UPDATE es_projection
                SET state = 'inline', after_transaction_id = pg_snapshot_xmax(pg_current_snapshot())
                WHERE projector = ?
                RETURNING after_transaction_id
                SQL);
            $lastTransactionIdStatement->execute([$projectorName]);
            $lastTransactionId = $lastTransactionIdStatement->fetchColumn();

            $this->connection->commit();
        } catch (\Throwable $e) {
            $this->connection->rollBack();
            throw $e;
        }

        // Execute missing events
        $missingEventsLoop = 0;
        $currentXminStatement = $this->connection->prepare('SELECT pg_snapshot_xmin(pg_current_snapshot())');
        while ($missingEventsLoop < $missingEventsMaxLoops) {
            $this->connection->beginTransaction();
            try {
                $currentXminStatement->execute();
                $currentXmin = $currentXminStatement->fetchColumn();
                foreach ($subscriptionLoader->read(new SubscriptionQuery(from: $lastPosition)) as $event) {
                    if ($event->eventId->transactionId > $lastTransactionId) {
                        break;
                    }
                    $projector->project($event);
                    $lastPosition = $event->eventId;
                }

                $this->connection->commit();
            } catch (\Throwable $e) {
                $this->connection->rollBack();
                throw $e;
            }

            if ($currentXmin > $lastTransactionId) {
                break;
            }

            $missingEventsLoop++;
            \sleep(1);
        }
    }

    public function lockState(string $projectionName): ?SubscriptionPosition
    {
        $statement = $this->connection->prepare(<<<SQL
            SELECT transaction_id, event_id FROM es_subscription
            WHERE projector = ?
            FOR UPDATE
            SQL);
        $statement->execute([$projectionName]);

        $position = $statement->fetch();

        return $position ? new SubscriptionPosition($position['transaction_id'], $position['event_id']) : null;
    }

    public function releaseState(string $projectionName, ?SubscriptionPosition $position): void
    {
        if (!$position) {
            return;
        }
        $this->connection->prepare(<<<SQL
            INSERT INTO es_subscription (transaction_id, event_id, projector)
            VALUES (?, ?, ?)
            ON CONFLICT DO
            UPDATE SET transaction_id = ?, event_id = ?
            WHERE projector = ?
            SQL)
            ->execute([
                $position->transactionId, $position->sequenceNumber, $projectionName,
                $position->transactionId, $position->sequenceNumber, $projectionName,
            ]);
    }

    private function getProjector(string $projectorName): Projector
    {
        $projector = $this->projectors[$projectorName] ?? null;
        if (!$projector) {
            throw new \RuntimeException('Unknown projector ' . $projectorName);
        }
        return $projector;
    }
}