<?php

declare(strict_types=1);

namespace DbalEs\Subscription;

use DbalEs\PersistedEvent;

interface SubscriptionLoader
{
    /**
     * @return iterable<PersistedEvent>
     */
    public function read(SubscriptionQuery $query): iterable;
}