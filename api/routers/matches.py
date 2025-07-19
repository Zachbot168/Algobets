"""
API routes for match data and information
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime, timedelta

from ..models.api_models import Match, MatchDetails, MatchListResponse, Team
from ingest.database import db

router = APIRouter()

@router.get("/matches", response_model=MatchListResponse)
async def get_matches(
    status: Optional[str] = Query(default=None, description="Match status filter"),
    days_ahead: int = Query(default=7, ge=1, le=30, description="Days ahead to look for upcoming matches"),
    days_back: int = Query(default=7, ge=1, le=90, description="Days back to look for completed matches"), 
    tournament: Optional[str] = Query(default=None, description="Tournament filter"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=100)
):
    """Get matches with filtering and pagination"""
    try:
        # Build query
        query = "SELECT COUNT(*) FROM bronze_matches WHERE 1=1"
        count_query = query
        
        query = """
            SELECT match_id, tournament_name, team_a_id, team_a_name, team_b_id, team_b_name,
                   start_time, status, best_of, patch_version, is_lan, winner_id
            FROM bronze_matches WHERE 1=1
        """
        
        params = []
        
        # Apply filters
        if status:
            query += " AND status = ?"
            count_query += " AND status = ?"
            params.append(status)
            
        if status == "upcoming" or (not status and days_ahead > 0):
            future_date = datetime.now() + timedelta(days=days_ahead)
            query += " AND start_time BETWEEN ? AND ?"
            count_query += " AND start_time BETWEEN ? AND ?"
            params.extend([datetime.now().isoformat(), future_date.isoformat()])
            
        elif status == "completed" or (not status and days_back > 0):
            past_date = datetime.now() - timedelta(days=days_back)
            query += " AND start_time BETWEEN ? AND ?"
            count_query += " AND start_time BETWEEN ? AND ?"
            params.extend([past_date.isoformat(), datetime.now().isoformat()])
            
        if tournament:
            query += " AND tournament_name ILIKE ?"
            count_query += " AND tournament_name ILIKE ?"
            params.append(f"%{tournament}%")
            
        # Get total count
        total_result = db.execute_query(count_query, params)
        total = total_result[0][0] if total_result else 0
        
        # Add pagination
        offset = (page - 1) * page_size
        query += " ORDER BY start_time DESC LIMIT ? OFFSET ?"
        params.extend([page_size, offset])
        
        results = db.execute_query(query, params)
        
        matches = []
        for result in results:
            # Create team objects
            team_a = Team(
                team_id=result[2],
                team_name=result[3]
            )
            team_b = Team(
                team_id=result[4], 
                team_name=result[5]
            )
            
            match = Match(
                match_id=result[0],
                tournament_name=result[1],
                team_a=team_a,
                team_b=team_b,
                start_time=datetime.fromisoformat(result[6]),
                status=result[7],
                best_of=result[8],
                patch_version=result[9],
                is_lan=bool(result[10])
            )
            matches.append(match)
            
        return MatchListResponse(
            matches=matches,
            total=total,
            page=page,
            page_size=page_size
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving matches: {str(e)}")

@router.get("/matches/{match_id}", response_model=MatchDetails)
async def get_match_details(match_id: str):
    """Get detailed information for a specific match"""
    try:
        # Get match basic info
        match_query = """
            SELECT match_id, tournament_name, team_a_id, team_a_name, team_b_id, team_b_name,
                   start_time, end_time, status, best_of, patch_version, is_lan, winner_id, venue
            FROM bronze_matches 
            WHERE match_id = ?
        """
        
        match_result = db.execute_query(match_query, [match_id])
        
        if not match_result:
            raise HTTPException(status_code=404, detail="Match not found")
            
        match_data = match_result[0]
        
        # Get maps for this match
        maps_query = """
            SELECT map_name, team_a_score, team_b_score, winner_id
            FROM bronze_maps 
            WHERE match_id = ?
            ORDER BY map_number
        """
        
        maps_result = db.execute_query(maps_query, [match_id])
        maps = [row[0] for row in maps_result] if maps_result else []
        
        # Get team rosters
        team_a_players = await get_team_roster(match_data[2])  # team_a_id
        team_b_players = await get_team_roster(match_data[4])  # team_b_id
        
        # Create team objects
        team_a = Team(
            team_id=match_data[2],
            team_name=match_data[3]
        )
        team_b = Team(
            team_id=match_data[4],
            team_name=match_data[5]
        )
        
        match_details = MatchDetails(
            match_id=match_data[0],
            tournament_name=match_data[1],
            team_a=team_a,
            team_b=team_b,
            start_time=datetime.fromisoformat(match_data[6]),
            status=match_data[8],
            best_of=match_data[9],
            patch_version=match_data[10],
            is_lan=bool(match_data[11]),
            maps=maps,
            team_a_players=team_a_players,
            team_b_players=team_b_players
        )
        
        return match_details
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving match details: {str(e)}")

@router.get("/matches/{match_id}/stats")
async def get_match_stats(match_id: str):
    """Get comprehensive statistics for a match"""
    try:
        # Get match maps with scores
        maps_query = """
            SELECT map_name, map_number, team_a_score, team_b_score, 
                   winner_id, duration
            FROM bronze_maps 
            WHERE match_id = ?
            ORDER BY map_number
        """
        
        maps_result = db.execute_query(maps_query, [match_id])
        
        if not maps_result:
            return {"error": "No map data found for this match"}
            
        maps_stats = []
        total_rounds = 0
        
        for map_data in maps_result:
            map_stats = {
                "map_name": map_data[0],
                "map_number": map_data[1],
                "team_a_score": map_data[2],
                "team_b_score": map_data[3],
                "winner_id": map_data[4],
                "duration_minutes": map_data[5] // 60 if map_data[5] else None,
                "total_rounds": map_data[2] + map_data[3]
            }
            
            total_rounds += map_data[2] + map_data[3]
            maps_stats.append(map_stats)
            
        # Get player stats if available
        player_stats_query = """
            SELECT player_name, team_id, agent, kills, deaths, assists,
                   first_bloods, acs, adr
            FROM bronze_player_stats ps
            JOIN bronze_maps m ON ps.map_id = m.map_id
            WHERE m.match_id = ?
        """
        
        player_result = db.execute_query(player_stats_query, [match_id])
        
        players_stats = []
        for player in player_result:
            players_stats.append({
                "player_name": player[0],
                "team_id": player[1],
                "agent": player[2],
                "kills": player[3],
                "deaths": player[4],
                "assists": player[5],
                "first_bloods": player[6],
                "acs": player[7],
                "adr": player[8]
            })
            
        return {
            "match_id": match_id,
            "maps": maps_stats,
            "players": players_stats,
            "summary": {
                "total_maps": len(maps_stats),
                "total_rounds": total_rounds,
                "average_rounds_per_map": total_rounds / len(maps_stats) if maps_stats else 0
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving match stats: {str(e)}")

@router.get("/matches/upcoming")
async def get_upcoming_matches(
    hours_ahead: int = Query(default=24, ge=1, le=168, description="Hours ahead to look"),
    tournament: Optional[str] = Query(default=None)
):
    """Get upcoming matches in the next specified hours"""
    try:
        end_time = datetime.now() + timedelta(hours=hours_ahead)
        
        query = """
            SELECT match_id, tournament_name, team_a_name, team_b_name, start_time, best_of
            FROM bronze_matches 
            WHERE start_time BETWEEN ? AND ? AND status = 'scheduled'
        """
        params = [datetime.now().isoformat(), end_time.isoformat()]
        
        if tournament:
            query += " AND tournament_name ILIKE ?"
            params.append(f"%{tournament}%")
            
        query += " ORDER BY start_time ASC"
        
        results = db.execute_query(query, params)
        
        upcoming_matches = []
        for result in results:
            upcoming_matches.append({
                "match_id": result[0],
                "tournament_name": result[1],
                "team_a_name": result[2],
                "team_b_name": result[3],
                "start_time": result[4],
                "best_of": result[5],
                "hours_until_start": (datetime.fromisoformat(result[4]) - datetime.now()).total_seconds() / 3600
            })
            
        return {
            "upcoming_matches": upcoming_matches,
            "total": len(upcoming_matches),
            "time_range_hours": hours_ahead
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving upcoming matches: {str(e)}")

# Helper functions
async def get_team_roster(team_id: str) -> List:
    """Get current roster for a team"""
    try:
        from ..models.api_models import Player
        
        query = """
            SELECT player_id, player_name, real_name, role, country
            FROM bronze_players 
            WHERE team_id = ? AND is_active = true
        """
        
        result = db.execute_query(query, [team_id])
        
        players = []
        for player_data in result:
            player = Player(
                player_id=player_data[0],
                player_name=player_data[1],
                real_name=player_data[2],
                team_id=team_id,
                role=player_data[3],
                country=player_data[4]
            )
            players.append(player)
            
        return players
        
    except Exception as e:
        return []