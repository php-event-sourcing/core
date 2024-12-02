<?php

declare(strict_types=1);

namespace DbalEs\Postgres;

use DbalEs\Dbal\Connection;
use DbalEs\PersistedEvent;
use DbalEs\Subscription\EventPage;
use DbalEs\Subscription\PersistentSubscriptions;
use DbalEs\Subscription\SubscriptionLoader;
use DbalEs\Subscription\SubscriptionPosition;
use DbalEs\Subscription\SubscriptionQuery;

class PostgresPersistentSubscriptions implements PersistentSubscriptions
{
    public const DEFAULT_BATCH_SIZE = 1000;
    public function __construct(
        private Connection $connection,
        private SubscriptionLoader $subscriptionLoader,
    ) {
    }

    public function createSubscription(string $subscriptionName, SubscriptionQuery $subscriptionQuery): void
    {
        $position = $subscriptionQuery->from ?? SubscriptionPosition::start();
        $this->connection->prepare(<<<SQL
            INSERT INTO es_subscription (transaction_id, event_id, projector, query)
            VALUES (?, ?, ?, ?)
            SQL)
            ->execute([
                $position->transactionId, $position->sequenceNumber, $subscriptionName, \json_encode($subscriptionQuery),
            ]);
    }

    public function deleteSubscription(string $subscriptionName): void
    {
        $this->connection->prepare(<<<SQL
            DELETE FROM es_subscription
            WHERE projector = ?
            SQL)
            ->execute([$subscriptionName]);
    }

    public function read(string $subscriptionName): EventPage
    {
        $statement = $this->connection->prepare(<<<SQL
            SELECT transaction_id, event_id, query
            FROM es_subscription
            WHERE projector = ?
            FOR UPDATE
            SQL);
        $statement->execute([$subscriptionName]);
        $row = $statement->fetch();
        if (!$row) {
            throw new \RuntimeException(\sprintf('Subscription "%s" not found', $subscriptionName));
        }
        $startPosition = new SubscriptionPosition((int) $row['transaction_id'], (int) $row['event_id']);
        $baseQueryData = \json_decode($row['query'], true);
        $baseQuery = new SubscriptionQuery(
            streamIds: $baseQueryData['streamIds'] ?? null,
            from: $startPosition,
            allowGaps: (bool) $baseQueryData['allowGaps'] ?? false,
            limit: (int) $baseQueryData['limit'] ?? self::DEFAULT_BATCH_SIZE,
        );
        $events = [];
        $position = null;
        /** @var PersistedEvent $event */
        foreach ($this->subscriptionLoader->read($baseQuery) as $event) {
            $events[] = $event;
            $position = $event->eventId;
        }

        return new EventPage(
            $subscriptionName,
            $events,
            $startPosition,
            $position ?? $startPosition,
            $baseQuery->limit);
    }

    public function ack(EventPage $page): void
    {
        // todo: ensure the transaction is not already acked
        $statement = $this->connection->prepare(<<<SQL
            UPDATE es_subscription
            SET transaction_id = ?, event_id = ?
            WHERE projector = ?
            SQL);
        $statement->execute([$page->endPosition->transactionId, $page->endPosition->sequenceNumber, $page->subscriptionName]);
        if ($statement->rowCount() === 0) {
            throw new \RuntimeException(\sprintf('Subscription "%s" not found', $page->subscriptionName));
        }
    }
}