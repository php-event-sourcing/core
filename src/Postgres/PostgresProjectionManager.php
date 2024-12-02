<?php

declare(strict_types=1);

namespace DbalEs\Postgres;

use DbalEs\Dbal\Connection;
use DbalEs\PersistedEvent;
use DbalEs\Projection\Projector;
use DbalEs\Projection\ProjectorWithSetup;
use DbalEs\Subscription\PersistentSubscriptions;
use DbalEs\Subscription\SubscriptionPosition;
use DbalEs\Subscription\SubscriptionQuery;

class PostgresProjectionManager
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

    public function catchupProjection(string $projectorName, PersistentSubscriptions $persistentSubscriptions, int $missingEventsMaxLoops = 100): void
    {
        $this->connection->beginTransaction();
        try {
            $statement = $this->connection->prepare(<<<SQL
                SELECT projector, state FROM es_projection
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

            $statement = $this->connection->prepare(<<<SQL
                UPDATE es_projection
                SET state = 'catching_up'
                WHERE projector = ?
                SQL);
            $statement->execute([$projectorName]);
            $this->connection->commit();
        } catch (\Throwable $e) {
            $this->connection->rollBack();
            throw $e;
        }

        $persistentSubscriptions->createSubscription($projectorName, new SubscriptionQuery(limit: 1000));
        do {
            $page = $persistentSubscriptions->read($projectorName);
            if ($page->events === []) {
                break;
            }
            $this->connection->beginTransaction();
            try {
                foreach ($page->events as $event) {
                    $projector->project($event);
                }
                $persistentSubscriptions->ack($page);
                $this->connection->commit();
            } catch (\Throwable $e) {
                $this->connection->rollBack();
                throw $e;
            }
        } while (true);

        $this->connection->beginTransaction();
        try {
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
                $page = $persistentSubscriptions->read($projectorName);
                foreach ($page->events as $event) {
                    if ($event->eventId->transactionId > $lastTransactionId) {
                        break;
                    }
                    $projector->project($event);
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
            \usleep(10000);
        }
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