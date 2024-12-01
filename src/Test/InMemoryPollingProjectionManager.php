<?php

declare(strict_types=1);

namespace DbalEs\Test;

use DbalEs\Projection\PollingProjectionManager;
use DbalEs\Subscription\SubscriptionPosition;

class InMemoryPollingProjectionManager implements PollingProjectionManager
{
    private array $states = [];

    public function lockState(string $projectionName): ?SubscriptionPosition
    {
        return $this->states[$projectionName] ?? null;
    }

    public function releaseState(string $projectionName, ?SubscriptionPosition $position): void
    {
        $this->states[$projectionName] = $position;
    }
}