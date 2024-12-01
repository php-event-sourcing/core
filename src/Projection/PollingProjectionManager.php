<?php

declare(strict_types=1);

namespace DbalEs\Projection;

use DbalEs\Subscription\SubscriptionPosition;

interface PollingProjectionManager
{
    public function lockState(string $projectionName): ?SubscriptionPosition;
    public function releaseState(string $projectionName, ?SubscriptionPosition $position): void;
}