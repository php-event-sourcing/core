<?php

declare(strict_types=1);

namespace DbalEs\Projection;

interface ProjectorWithSetup extends Projector
{
    public function setUp(): void;
    public function tearDown(): void;
}