"""
Demo Mode API Endpoints.

Provides endpoints to control the demo data generator for
presentations and testing when live data is unavailable.
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.demo_generator import demo_generator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/demo", tags=["Demo Mode"])


# === Request/Response Models ===

class DemoStartRequest(BaseModel):
    """Request model for starting demo mode."""
    scenario: str = Field(
        default="viral_spike",
        description="Demo scenario to run"
    )
    interval: float = Field(
        default=3.0,
        ge=1.0,
        le=30.0,
        description="Seconds between data points"
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "scenario": "viral_spike",
                    "interval": 2.0
                }
            ]
        }
    }


class DemoStatusResponse(BaseModel):
    """Response model for demo status."""
    running: bool = Field(description="Whether demo is running")
    scenario: str = Field(description="Current scenario name")
    interval: float = Field(description="Current interval in seconds")
    iteration: int = Field(description="Number of iterations completed")
    active_trends: int = Field(description="Number of active trends")
    trends: dict = Field(default_factory=dict, description="Current trend data")


class DemoScenarioInfo(BaseModel):
    """Information about a demo scenario."""
    name: str = Field(description="Scenario identifier")
    title: str = Field(description="Human-readable title")
    description: str = Field(description="Scenario description")
    recommended_interval: float = Field(description="Recommended interval in seconds")


class DemoActionResponse(BaseModel):
    """Response for demo actions."""
    success: bool
    message: str
    status: Optional[DemoStatusResponse] = None


# === Endpoints ===

@router.post("/start", response_model=DemoActionResponse)
async def start_demo(request: DemoStartRequest):
    """
    Start the demo data generator.

    Begins generating realistic mock social media data and publishing
    it to the Kafka market-stream topic.
    """
    if demo_generator.is_running:
        return DemoActionResponse(
            success=False,
            message="Demo generator is already running. Stop it first or use /scenario to switch.",
            status=DemoStatusResponse(**demo_generator.stats)
        )

    success = await demo_generator.start(
        scenario=request.scenario,
        interval=request.interval
    )

    if success:
        return DemoActionResponse(
            success=True,
            message=f"Demo started with scenario '{request.scenario}'",
            status=DemoStatusResponse(**demo_generator.stats)
        )
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to start demo. Invalid scenario: {request.scenario}"
        )


@router.post("/stop", response_model=DemoActionResponse)
async def stop_demo():
    """
    Stop the demo data generator.

    Stops all demo data generation. The Kafka topic will no longer
    receive new mock data.
    """
    if not demo_generator.is_running:
        return DemoActionResponse(
            success=False,
            message="Demo generator is not running"
        )

    await demo_generator.stop()

    return DemoActionResponse(
        success=True,
        message="Demo generator stopped"
    )


@router.get("/status", response_model=DemoStatusResponse)
async def get_demo_status():
    """
    Get current demo generator status.

    Returns information about whether the generator is running,
    current scenario, and active trend data.
    """
    return DemoStatusResponse(**demo_generator.stats)


@router.post("/scenario/{scenario_name}", response_model=DemoActionResponse)
async def switch_scenario(scenario_name: str):
    """
    Switch to a different demo scenario.

    Available scenarios:
    - viral_spike: Hashtag explodes from thousands to millions
    - steady_growth: Multiple trends with consistent growth
    - declining: Content past its peak showing decay
    - multi_trend: Multiple hashtags competing
    - oscillating: Wave-like engagement pattern
    """
    if not demo_generator.is_running:
        return DemoActionResponse(
            success=False,
            message="Demo generator is not running. Start it first."
        )

    success = demo_generator.set_scenario(scenario_name)

    if success:
        return DemoActionResponse(
            success=True,
            message=f"Switched to scenario '{scenario_name}'",
            status=DemoStatusResponse(**demo_generator.stats)
        )
    else:
        available = [s["name"] for s in demo_generator.get_available_scenarios()]
        raise HTTPException(
            status_code=400,
            detail=f"Invalid scenario '{scenario_name}'. Available: {available}"
        )


@router.get("/scenarios", response_model=list[DemoScenarioInfo])
async def list_scenarios():
    """
    List all available demo scenarios.

    Returns detailed information about each scenario including
    description and recommended settings.
    """
    return [
        DemoScenarioInfo(**scenario)
        for scenario in demo_generator.get_available_scenarios()
    ]


@router.post("/trigger", response_model=DemoActionResponse)
async def trigger_single_update():
    """
    Trigger a single demo data update.

    Useful for testing without starting the continuous generator.
    Generates one data point for all active trends.
    """
    if not demo_generator.is_running:
        # Start temporarily, generate one update, then stop
        await demo_generator.start(scenario="viral_spike", interval=999)
        await demo_generator.stop()

        return DemoActionResponse(
            success=True,
            message="Single demo update triggered"
        )

    return DemoActionResponse(
        success=True,
        message="Generator is running - continuous updates are being generated",
        status=DemoStatusResponse(**demo_generator.stats)
    )
