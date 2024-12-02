<?php

declare(strict_types=1);

namespace DbalEs\Subscription;

use Closure;
use DbalEs\PersistedEvent;

readonly class EventPage
{
    /**
     * @param array<PersistedEvent> $events
     */
    public function __construct(
        public string $subscriptionName,
        public array $events,
        public SubscriptionPosition $startPosition,
        public SubscriptionPosition $endPosition,
        public int $requestedBatchSize,
    ) {
    }
}