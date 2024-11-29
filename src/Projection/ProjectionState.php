<?php

declare(strict_types=1);

namespace DbalEs\Projection;

use DbalEs\Subscription\SubscriptionPosition;

readonly class ProjectionState
{
    public function __construct(
        public ?SubscriptionPosition $position = null,
    ) {
    }

    public function withPosition(?SubscriptionPosition $position)
    {
        return new self(position: $position);
    }
}