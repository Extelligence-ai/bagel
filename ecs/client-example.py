"""
Example: Connecting to Bagel MCP Server from another project

This example shows how to connect to the Bagel MCP Server running on ECS
and use its trajectory analysis functions.
"""

import asyncio
import httpx
from typing import Any, Dict, List


class BagelMCPClient:
    """Client for connecting to Bagel MCP Server on ECS"""
    
    def __init__(self, base_url: str):
        """
        Initialize the client.
        
        Args:
            base_url: Base URL of the MCP server (e.g., "http://your-alb-dns-name:8000")
        """
        self.base_url = base_url.rstrip('/')
        self.client = httpx.AsyncClient(timeout=60.0)
    
    async def analyze_trajectory(
        self, 
        robolog_path: str, 
        start_seconds: float | None = None,
        end_seconds: float | None = None
    ) -> Dict[str, Any]:
        """
        Analyze trajectory data from a robotics log.
        
        Args:
            robolog_path: Path to the robolog file
            start_seconds: Optional start time filter
            end_seconds: Optional end time filter
            
        Returns:
            Trajectory analysis results
        """
        # Note: This is a simplified example
        # In practice, you'd use the MCP protocol to call the tool
        # For SSE transport, you'd establish an SSE connection first
        
        payload = {
            "robolog_path": robolog_path,
            "start_seconds": start_seconds,
            "end_seconds": end_seconds
        }
        
        # MCP tools are typically called via SSE or HTTP POST
        # This is a placeholder - actual implementation depends on MCP client library
        response = await self.client.post(
            f"{self.base_url}/tools/analyze_trajectory",
            json=payload
        )
        response.raise_for_status()
        return response.json()
    
    async def compare_trajectory(
        self,
        robolog_path: str,
        start_seconds: float | None = None,
        end_seconds: float | None = None
    ) -> Dict[str, Any]:
        """Compare planned vs actual trajectory."""
        payload = {
            "robolog_path": robolog_path,
            "start_seconds": start_seconds,
            "end_seconds": end_seconds
        }
        
        response = await self.client.post(
            f"{self.base_url}/tools/compare_trajectory",
            json=payload
        )
        response.raise_for_status()
        return response.json()
    
    async def read_metadata(self, robolog_path: str) -> Dict[str, Any]:
        """Read metadata from a robolog."""
        payload = {"robolog_path": robolog_path}
        
        response = await self.client.post(
            f"{self.base_url}/tools/read_metadata",
            json=payload
        )
        response.raise_for_status()
        return response.json()
    
    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()


# Example usage
async def main():
    # Initialize client with your ECS endpoint
    # Replace with your actual ALB DNS name or ECS endpoint
    client = BagelMCPClient("http://your-bagel-alb-123456789.us-east-1.elb.amazonaws.com:8000")
    
    try:
        # Example 1: Read metadata
        print("Reading metadata...")
        metadata = await client.read_metadata("doc/tutorials/data/px4.ulg")
        print(f"Duration: {metadata.get('duration_seconds')} seconds")
        print(f"Topics: {len(metadata.get('topics', []))}")
        
        # Example 2: Analyze trajectory
        print("\nAnalyzing trajectory...")
        analysis = await client.analyze_trajectory(
            "doc/tutorials/data/px4.ulg",
            start_seconds=0,
            end_seconds=100
        )
        print(f"Analysis complete: {analysis}")
        
        # Example 3: Compare trajectory
        print("\nComparing trajectory...")
        comparison = await client.compare_trajectory(
            "doc/tutorials/data/px4.ulg"
        )
        print(f"Comparison complete: {comparison}")
        
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
