<?php

declare(strict_types=1);

namespace DbalEs\Subscription;

readonly class SubscriptionPosition
{
    public function __construct(
        public int $transactionId,
        public int $sequenceNumber,
    ) {
    }

    public static function start(): self
    {
        return new self(0, 0);
    }
}