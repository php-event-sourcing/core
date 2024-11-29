<?php

declare(strict_types=1);

namespace DbalEs;

use DbalEs\Subscription\SubscriptionPosition;

readonly class PersistedEvent
{
    public function __construct(
        public SubscriptionPosition $eventId,
        public Event $event,
        public EventStreamId $eventStreamId,
    ) {
    }
}