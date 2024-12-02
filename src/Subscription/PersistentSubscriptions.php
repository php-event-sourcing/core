<?php

declare(strict_types=1);

namespace DbalEs\Subscription;

interface PersistentSubscriptions
{
    public function createSubscription(string $subscriptionName, SubscriptionQuery $subscriptionQuery): void;
    public function deleteSubscription(string $subscriptionName): void;
    public function read(string $subscriptionName): EventPage;
    public function ack(EventPage $page): void;
}