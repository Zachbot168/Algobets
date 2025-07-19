"""
API routes for teams and players information
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime, timedelta

from ..models.api_models import Team, Player
from ingest.database import db

router = APIRouter()

@router.get("/teams", response_model=List[Team])
async def get_teams(
    region: Optional[str] = Query(default=None, description="Filter by region"),
    country: Optional[str] = Query(default=None, description="Filter by country"),
    active_only: bool = Query(default=True, description="Only return active teams"),
    limit: int = Query(default=100, ge=1, le=500)
):
    """Get list of teams with optional filtering"""
    try:
        query = """
            SELECT team_id, team_name, region, country, logo_url, is_active
            FROM bronze_teams 
            WHERE 1=1
        """
        params = []
        
        if active_only:
            query += " AND is_active = true"
            
        if region:
            query += " AND region ILIKE ?"
            params.append(f"%{region}%")
            
        if country:
            query += " AND country ILIKE ?"
            params.append(f"%{country}%")
            
        query += " ORDER BY team_name LIMIT ?"
        params.append(limit)
        
        results = db.execute_query(query, params)
        
        teams = []
        for result in results:
            team = Team(
                team_id=result[0],
                team_name=result[1],
                region=result[2],
                country=result[3],
                logo_url=result[4]
            )
            teams.append(team)
            
        return teams
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving teams: {str(e)}")

@router.get("/teams/{team_id}", response_model=Team)
async def get_team_details(team_id: str):
    """Get detailed information for a specific team"""
    try:
        query = """
            SELECT team_id, team_name, region, country, logo_url, is_active
            FROM bronze_teams 
            WHERE team_id = ?
        """
        
        result = db.execute_query(query, [team_id])
        
        if not result:
            raise HTTPException(status_code=404, detail="Team not found")
            
        team_data = result[0]
        team = Team(
            team_id=team_data[0],
            team_name=team_data[1],
            region=team_data[2],
            country=team_data[3],
            logo_url=team_data[4]
        )
        
        return team
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving team details: {str(e)}")

@router.get("/teams/{team_id}/roster", response_model=List[Player])
async def get_team_roster(team_id: str, active_only: bool = Query(default=True)):
    """Get current roster for a team"""
    try:
        query = """
            SELECT player_id, player_name, real_name, team_id, role, country,
                   join_date, leave_date, is_active
            FROM bronze_players 
            WHERE team_id = ?
        """
        params = [team_id]
        
        if active_only:
            query += " AND is_active = true"
            
        query += " ORDER BY role, player_name"
        
        results = db.execute_query(query, params)
        
        players = []
        for result in results:
            player = Player(
                player_id=result[0],
                player_name=result[1],
                real_name=result[2],
                team_id=result[3],
                role=result[4],
                country=result[5]
            )
            players.append(player)
            
        return players
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving team roster: {str(e)}")

@router.get("/teams/{team_id}/matches")
async def get_team_matches(
    team_id: str,
    days_back: int = Query(default=90, ge=1, le=365, description="Days back to look for matches"),
    status: Optional[str] = Query(default=None, description="Match status filter"),
    limit: int = Query(default=50, ge=1, le=200)
):
    """Get recent matches for a team"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        query = """
            SELECT match_id, tournament_name, team_a_id, team_a_name, 
                   team_b_id, team_b_name, start_time, status, winner_id,
                   CASE WHEN team_a_id = ? THEN team_b_name ELSE team_a_name END as opponent
            FROM bronze_matches 
            WHERE (team_a_id = ? OR team_b_id = ?) AND start_time >= ?
        """
        params = [team_id, team_id, team_id, cutoff_date.isoformat()]
        
        if status:
            query += " AND status = ?"
            params.append(status)
            
        query += " ORDER BY start_time DESC LIMIT ?"
        params.append(limit)
        
        results = db.execute_query(query, params)
        
        matches = []
        for result in results:
            # Determine if team won
            won = None
            if result[8]:  # winner_id exists
                won = result[8] == team_id
                
            matches.append({
                "match_id": result[0],
                "tournament_name": result[1],
                "opponent": result[9],
                "start_time": result[6],
                "status": result[7],
                "won": won,
                "was_team_a": result[2] == team_id
            })
            
        return {
            "team_id": team_id,
            "matches": matches,
            "total": len(matches),
            "days_back": days_back
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving team matches: {str(e)}")

@router.get("/teams/{team_id}/stats")
async def get_team_stats(
    team_id: str,
    days_back: int = Query(default=90, ge=30, le=365, description="Days back for statistics")
):
    """Get comprehensive team statistics"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        # Basic match statistics
        match_stats_query = """
            SELECT 
                COUNT(*) as total_matches,
                SUM(CASE WHEN winner_id = ? THEN 1 ELSE 0 END) as wins,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_matches
            FROM bronze_matches 
            WHERE (team_a_id = ? OR team_b_id = ?) 
              AND start_time >= ? 
              AND status = 'completed'
        """
        
        match_stats = db.execute_query(match_stats_query, [team_id, team_id, team_id, cutoff_date.isoformat()])
        
        if not match_stats or match_stats[0][0] == 0:
            return {
                "team_id": team_id,
                "error": "No match data found for this time period"
            }
            
        total_matches, wins, completed = match_stats[0]
        win_rate = wins / completed if completed > 0 else 0
        
        # Map statistics
        map_stats_query = """
            SELECT 
                m.map_name,
                COUNT(*) as maps_played,
                SUM(CASE WHEN m.winner_id = ? THEN 1 ELSE 0 END) as maps_won,
                AVG(CASE WHEN ma.team_a_id = ? THEN m.team_a_score ELSE m.team_b_score END) as avg_rounds_for,
                AVG(CASE WHEN ma.team_a_id = ? THEN m.team_b_score ELSE m.team_a_score END) as avg_rounds_against
            FROM bronze_maps m
            JOIN bronze_matches ma ON m.match_id = ma.match_id
            WHERE (ma.team_a_id = ? OR ma.team_b_id = ?) 
              AND ma.start_time >= ?
            GROUP BY m.map_name
            ORDER BY maps_played DESC
        """
        
        map_stats = db.execute_query(map_stats_query, [
            team_id, team_id, team_id, team_id, team_id, cutoff_date.isoformat()
        ])
        
        maps_performance = []
        for map_stat in map_stats:
            maps_performance.append({
                "map_name": map_stat[0],
                "maps_played": map_stat[1],
                "maps_won": map_stat[2],
                "win_rate": map_stat[2] / map_stat[1] if map_stat[1] > 0 else 0,
                "avg_rounds_for": round(map_stat[3], 1) if map_stat[3] else 0,
                "avg_rounds_against": round(map_stat[4], 1) if map_stat[4] else 0
            })
            
        # Recent form (last 10 matches)
        recent_form_query = """
            SELECT winner_id = ? as won
            FROM bronze_matches 
            WHERE (team_a_id = ? OR team_b_id = ?) 
              AND status = 'completed'
            ORDER BY start_time DESC 
            LIMIT 10
        """
        
        recent_form = db.execute_query(recent_form_query, [team_id, team_id, team_id])
        form_string = ''.join(['W' if match[0] else 'L' for match in recent_form])
        
        return {
            "team_id": team_id,
            "period_days": days_back,
            "overall_stats": {
                "total_matches": total_matches,
                "wins": wins,
                "losses": completed - wins,
                "win_rate": round(win_rate, 3),
                "completed_matches": completed
            },
            "maps_performance": maps_performance,
            "recent_form": {
                "form_string": form_string,
                "last_10_wins": sum(1 for match in recent_form if match[0]),
                "last_10_matches": len(recent_form)
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving team stats: {str(e)}")

@router.get("/players/{player_id}", response_model=Player)
async def get_player_details(player_id: str):
    """Get detailed information for a specific player"""
    try:
        query = """
            SELECT player_id, player_name, real_name, team_id, role, country,
                   join_date, leave_date, is_active
            FROM bronze_players 
            WHERE player_id = ?
        """
        
        result = db.execute_query(query, [player_id])
        
        if not result:
            raise HTTPException(status_code=404, detail="Player not found")
            
        player_data = result[0]
        player = Player(
            player_id=player_data[0],
            player_name=player_data[1],
            real_name=player_data[2],
            team_id=player_data[3],
            role=player_data[4],
            country=player_data[5]
        )
        
        return player
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving player details: {str(e)}")

@router.get("/players")
async def search_players(
    name: Optional[str] = Query(default=None, description="Search by player name"),
    team_id: Optional[str] = Query(default=None, description="Filter by team"),
    role: Optional[str] = Query(default=None, description="Filter by role"),
    country: Optional[str] = Query(default=None, description="Filter by country"),
    active_only: bool = Query(default=True),
    limit: int = Query(default=50, ge=1, le=200)
):
    """Search for players with various filters"""
    try:
        query = """
            SELECT p.player_id, p.player_name, p.real_name, p.team_id, 
                   p.role, p.country, t.team_name
            FROM bronze_players p
            LEFT JOIN bronze_teams t ON p.team_id = t.team_id
            WHERE 1=1
        """
        params = []
        
        if active_only:
            query += " AND p.is_active = true"
            
        if name:
            query += " AND (p.player_name ILIKE ? OR p.real_name ILIKE ?)"
            params.extend([f"%{name}%", f"%{name}%"])
            
        if team_id:
            query += " AND p.team_id = ?"
            params.append(team_id)
            
        if role:
            query += " AND p.role ILIKE ?"
            params.append(f"%{role}%")
            
        if country:
            query += " AND p.country ILIKE ?"
            params.append(f"%{country}%")
            
        query += " ORDER BY p.player_name LIMIT ?"
        params.append(limit)
        
        results = db.execute_query(query, params)
        
        players = []
        for result in results:
            players.append({
                "player_id": result[0],
                "player_name": result[1],
                "real_name": result[2],
                "team_id": result[3],
                "role": result[4],
                "country": result[5],
                "team_name": result[6]
            })
            
        return {
            "players": players,
            "total": len(players),
            "filters_applied": {
                "name": name,
                "team_id": team_id,
                "role": role,
                "country": country,
                "active_only": active_only
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching players: {str(e)}")

@router.get("/regions")
async def get_regions():
    """Get list of all regions with team counts"""
    try:
        query = """
            SELECT region, COUNT(*) as team_count
            FROM bronze_teams 
            WHERE region IS NOT NULL AND is_active = true
            GROUP BY region
            ORDER BY team_count DESC
        """
        
        results = db.execute_query(query)
        
        regions = []
        for result in results:
            regions.append({
                "region": result[0],
                "team_count": result[1]
            })
            
        return {
            "regions": regions,
            "total_regions": len(regions)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving regions: {str(e)}")