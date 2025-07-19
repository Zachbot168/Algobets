"""
API routes for betting odds and market analysis
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime, timedelta

from ..models.api_models import Odds, OddsComparison
from ..services.odds_service import OddsService

router = APIRouter()

# Initialize service
odds_service = OddsService()

@router.get("/odds/{match_id}")
async def get_match_odds(
    match_id: str,
    market_type: Optional[str] = Query(default=None, description="Filter by market type"),
    bookmaker: Optional[str] = Query(default=None, description="Filter by bookmaker"),
    latest_only: bool = Query(default=True, description="Only return latest odds")
):
    """Get all odds for a specific match"""
    try:
        from ingest.database import db
        
        # Build query
        query = """
            SELECT odds_id, match_id, bookmaker, market_type, selection,
                   odds_decimal, odds_american, timestamp, is_latest
            FROM bronze_odds 
            WHERE match_id = ?
        """
        params = [match_id]
        
        if market_type:
            query += " AND market_type = ?"
            params.append(market_type)
            
        if bookmaker:
            query += " AND bookmaker = ?"
            params.append(bookmaker)
            
        if latest_only:
            query += " AND is_latest = true"
            
        query += " ORDER BY market_type, selection, odds_decimal DESC"
        
        results = db.execute_query(query, params)
        
        if not results:
            raise HTTPException(status_code=404, detail="No odds found for this match")
            
        odds_list = []
        for result in results:
            odds = Odds(
                odds_id=result[0],
                match_id=result[1],
                bookmaker=result[2],
                market_type=result[3],
                selection=result[4],
                odds_decimal=result[5],
                odds_american=result[6],
                timestamp=datetime.fromisoformat(result[7]),
                is_latest=bool(result[8])
            )
            odds_list.append(odds)
            
        return odds_list
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving odds: {str(e)}")

@router.get("/odds/{match_id}/comparison")
async def get_odds_comparison(
    match_id: str,
    market_type: str = Query(..., description="Market type to compare")
):
    """Get odds comparison across all bookmakers for a specific market"""
    try:
        comparison_data = await odds_service.get_odds_comparison(match_id, market_type)
        
        if not comparison_data:
            raise HTTPException(status_code=404, detail="No odds found for this market")
            
        # Group by selection
        selections = {}
        for odds in comparison_data:
            selection = odds['selection']
            if selection not in selections:
                selections[selection] = {
                    'selection': selection,
                    'best_odds': odds['odds_decimal'],
                    'best_bookmaker': odds['bookmaker'],
                    'all_odds': [],
                    'implied_probability': odds['implied_probability']
                }
            else:
                if odds['odds_decimal'] > selections[selection]['best_odds']:
                    selections[selection]['best_odds'] = odds['odds_decimal']
                    selections[selection]['best_bookmaker'] = odds['bookmaker']
                    
            selections[selection]['all_odds'].append(odds)
            
        comparison_results = list(selections.values())
        
        return {
            "match_id": match_id,
            "market_type": market_type,
            "comparisons": comparison_results,
            "total_bookmakers": len(set(odds['bookmaker'] for odds in comparison_data))
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting odds comparison: {str(e)}")

@router.get("/odds/{match_id}/movement")
async def get_odds_movement(
    match_id: str,
    market_type: str = Query(..., description="Market type to analyze"),
    hours_back: int = Query(default=24, ge=1, le=168, description="Hours back to analyze")
):
    """Get odds movement over time for a specific market"""
    try:
        movement_data = await odds_service.get_odds_movement(match_id, market_type, hours_back)
        
        if not movement_data:
            raise HTTPException(status_code=404, detail="No odds movement data found")
            
        # Organize by selection and bookmaker
        movement_by_selection = {}
        
        for odds in movement_data:
            key = f"{odds['selection']}_{odds['bookmaker']}"
            if key not in movement_by_selection:
                movement_by_selection[key] = {
                    'selection': odds['selection'],
                    'bookmaker': odds['bookmaker'],
                    'data_points': []
                }
                
            movement_by_selection[key]['data_points'].append({
                'odds_decimal': odds['odds_decimal'],
                'timestamp': odds['timestamp']
            })
            
        # Calculate movement statistics
        for key, data in movement_by_selection.items():
            data_points = data['data_points']
            if len(data_points) >= 2:
                first_odds = data_points[0]['odds_decimal']
                last_odds = data_points[-1]['odds_decimal']
                
                data['opening_odds'] = first_odds
                data['current_odds'] = last_odds
                data['movement_percentage'] = ((last_odds - first_odds) / first_odds) * 100
                data['total_data_points'] = len(data_points)
                
        return {
            "match_id": match_id,
            "market_type": market_type,
            "hours_analyzed": hours_back,
            "movement_data": list(movement_by_selection.values())
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting odds movement: {str(e)}")

@router.get("/odds/{match_id}/best")
async def get_best_odds(
    match_id: str,
    market_type: str = Query(..., description="Market type"),
    selection: Optional[str] = Query(default=None, description="Specific selection")
):
    """Get the best available odds for a market"""
    try:
        best_odds = await odds_service.get_best_odds(match_id, market_type, selection)
        
        if not best_odds:
            raise HTTPException(status_code=404, detail="No odds found for this market")
            
        return best_odds
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting best odds: {str(e)}")

@router.get("/odds/{match_id}/arbitrage")
async def get_arbitrage_opportunities(match_id: str):
    """Find arbitrage opportunities for a match"""
    try:
        arbitrage_opps = await odds_service.calculate_arbitrage_opportunities(match_id)
        
        return {
            "match_id": match_id,
            "arbitrage_opportunities": arbitrage_opps,
            "total_opportunities": len(arbitrage_opps)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculating arbitrage: {str(e)}")

@router.get("/market-analysis/{market_type}")
async def get_market_analysis(
    market_type: str,
    days_back: int = Query(default=30, ge=7, le=90, description="Days back to analyze")
):
    """Get market efficiency analysis for a specific market type"""
    try:
        efficiency_data = await odds_service.get_market_efficiency(market_type, days_back)
        
        if 'error' in efficiency_data:
            raise HTTPException(status_code=404, detail=efficiency_data['error'])
            
        return efficiency_data
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error analyzing market: {str(e)}")

@router.get("/bookmakers/{bookmaker}/limits")
async def get_bookmaker_limits(bookmaker: str):
    """Get betting limits and information for a specific bookmaker"""
    try:
        limits = await odds_service.get_bookmaker_limits(bookmaker)
        
        return {
            "bookmaker": bookmaker,
            "limits": limits,
            "last_updated": datetime.now()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting bookmaker limits: {str(e)}")

@router.get("/closing-line-value/{match_id}")
async def get_closing_line_value(
    match_id: str,
    market_type: str = Query(..., description="Market type to analyze")
):
    """Calculate closing line value for a completed match"""
    try:
        clv_data = await odds_service.get_closing_line_value(match_id, market_type)
        
        if 'error' in clv_data:
            raise HTTPException(status_code=404, detail=clv_data['error'])
            
        return clv_data
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculating CLV: {str(e)}")

@router.get("/odds/recent")
async def get_recent_odds(
    hours_back: int = Query(default=6, ge=1, le=48, description="Hours back to retrieve"),
    market_type: Optional[str] = Query(default=None),
    bookmaker: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500)
):
    """Get recent odds updates across all matches"""
    try:
        from ingest.database import db
        
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        
        query = """
            SELECT DISTINCT o.odds_id, o.match_id, o.bookmaker, o.market_type, 
                   o.selection, o.odds_decimal, o.timestamp, m.team_a_name, m.team_b_name
            FROM bronze_odds o
            JOIN bronze_matches m ON o.match_id = m.match_id
            WHERE o.timestamp >= ? AND o.is_latest = true
        """
        params = [cutoff_time.isoformat()]
        
        if market_type:
            query += " AND o.market_type = ?"
            params.append(market_type)
            
        if bookmaker:
            query += " AND o.bookmaker = ?"
            params.append(bookmaker)
            
        query += " ORDER BY o.timestamp DESC LIMIT ?"
        params.append(limit)
        
        results = db.execute_query(query, params)
        
        recent_odds = []
        for result in results:
            recent_odds.append({
                "odds_id": result[0],
                "match_id": result[1],
                "bookmaker": result[2],
                "market_type": result[3],
                "selection": result[4],
                "odds_decimal": result[5],
                "timestamp": result[6],
                "team_a_name": result[7],
                "team_b_name": result[8]
            })
            
        return {
            "recent_odds": recent_odds,
            "total": len(recent_odds),
            "hours_back": hours_back
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving recent odds: {str(e)}")