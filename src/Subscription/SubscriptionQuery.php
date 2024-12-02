<?php

declare(strict_types=1);

namespace DbalEs\Subscription;

readonly class SubscriptionQuery
{
    public function __construct(
        public array $streamIds = [],
        public ?SubscriptionPosition $from = null,
        public bool $allowGaps = false,
        public ?int $limit = null,
    ) {
    }

    public function withPosition(?SubscriptionPosition $position)
    {
        return new self(
            $this->streamIds,
            $position,
            $this->allowGaps,
            $this->limit,
        );
    }
}