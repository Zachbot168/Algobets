"""
Patch analysis and impact scoring with GPT-4
"""

import os
import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging
import openai

from ..base_ingester import BaseIngester

logger = logging.getLogger(__name__)

class PatchIngester(BaseIngester):
    """Ingests and analyzes VALORANT patch data with GPT-4 impact scoring"""
    
    def __init__(self):
        super().__init__(rate_limit_per_minute=10)  # Conservative for OpenAI API
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        self.data_dragon_url = "https://valorant-api.com"
        
        # Initialize OpenAI client
        if self.openai_api_key:
            openai.api_key = self.openai_api_key
        
    def get_default_headers(self) -> Dict[str, str]:
        return {
            'User-Agent': 'VALORANT-Betting-Platform/1.0',
            'Accept': 'application/json'
        }
        
    async def ingest_data(self, **kwargs) -> Dict[str, Any]:
        """Ingest and analyze patch data with GPT-4 scoring"""
        start_time = self.get_current_timestamp()
        
        try:
            if not self.openai_api_key:
                logger.warning("OpenAI API key not configured, skipping patch analysis")
                return {'patches_count': 0, 'errors': 1, 'source': 'patch_analysis'}
                
            # Get patch data from multiple sources
            patch_data = await self.fetch_patch_data()
            
            if not patch_data:
                logger.warning("No patch data retrieved")
                return {'patches_count': 0, 'errors': 0, 'source': 'patch_analysis'}
                
            # Analyze patches with GPT-4
            analyzed_patches = await self.analyze_patches_with_gpt4(patch_data)
            
            # Save to database
            saved_patches = self.save_patches_to_bronze(analyzed_patches)
            
            # Update matches with patch information
            await self.update_matches_with_patch_info(analyzed_patches)
            
            self.log_ingestion_stats("patch_analysis", saved_patches, start_time)
            
            return {
                'patches_count': saved_patches,
                'source': 'patch_analysis',
                'errors': 0
            }
            
        except Exception as e:
            logger.error(f"Error in patch analysis: {e}")
            self.log_ingestion_stats("patch_analysis", 0, start_time, errors=1)
            raise
            
    async def fetch_patch_data(self) -> List[Dict]:
        """Fetch patch data from VALORANT API and other sources"""
        try:
            # Fetch from VALORANT API
            api_patches = await self.fetch_from_valorant_api()
            
            # Fetch from community sources (VLR.gg patch notes)
            community_patches = await self.fetch_from_community_sources()
            
            # Combine and deduplicate
            all_patches = []
            seen_versions = set()
            
            for patches in [api_patches, community_patches]:
                for patch in patches:
                    version = patch.get('version')
                    if version and version not in seen_versions:
                        all_patches.append(patch)
                        seen_versions.add(version)
                        
            return all_patches
            
        except Exception as e:
            logger.error(f"Error fetching patch data: {e}")
            return []
            
    async def fetch_from_valorant_api(self) -> List[Dict]:
        """Fetch patch data from VALORANT API"""
        try:
            # Get version data
            version_url = f"{self.data_dragon_url}/v1/version"
            version_response = await self.make_request(version_url)
            
            if not version_response:
                return []
                
            current_version = version_response.get('data', {}).get('version')
            
            # Get patch notes if available
            patches = []
            if current_version:
                patch_data = {
                    'version': current_version,
                    'release_date': datetime.now().date().isoformat(),
                    'source': 'valorant_api',
                    'patch_notes': 'Current game version data',
                    'agent_changes': None,
                    'map_changes': None,
                    'weapon_changes': None
                }
                patches.append(patch_data)
                
            return patches
            
        except Exception as e:
            logger.error(f"Error fetching from VALORANT API: {e}")
            return []
            
    async def fetch_from_community_sources(self) -> List[Dict]:
        """Fetch patch notes from community sources like VLR.gg"""
        try:
            # This would typically scrape patch notes from VLR.gg or other sources
            # For now, return empty as this requires web scraping
            patches = []
            
            # Example structure of what would be scraped:
            # {
            #     'version': '7.04',
            #     'release_date': '2023-08-29',
            #     'source': 'vlr_gg',
            #     'patch_notes': 'Full patch notes text...',
            #     'agent_changes': 'Agent balance changes...',
            #     'map_changes': 'Map updates...',
            #     'weapon_changes': 'Weapon balance...'
            # }
            
            return patches
            
        except Exception as e:
            logger.error(f"Error fetching from community sources: {e}")
            return []
            
    async def analyze_patches_with_gpt4(self, patches: List[Dict]) -> List[Dict]:
        """Analyze patches with GPT-4 to generate impact scores"""
        analyzed_patches = []
        
        for patch in patches:
            try:
                # Prepare patch content for analysis
                patch_content = self.prepare_patch_content(patch)
                
                # Generate GPT-4 analysis
                analysis = await self.generate_gpt4_analysis(patch_content)
                
                # Extract impact score and summary
                impact_score, summary = self.extract_impact_info(analysis)
                
                # Create analyzed patch record
                analyzed_patch = {
                    'patch_id': f"patch_{patch['version'].replace('.', '_')}",
                    'version': patch['version'],
                    'release_date': patch['release_date'],
                    'patch_notes': patch.get('patch_notes', ''),
                    'agent_changes': patch.get('agent_changes'),
                    'map_changes': patch.get('map_changes'),
                    'weapon_changes': patch.get('weapon_changes'),
                    'llm_summary': summary,
                    'impact_score': impact_score,
                    'ingested_at': self.get_current_timestamp()
                }
                
                analyzed_patches.append(analyzed_patch)
                
                # Rate limiting for OpenAI
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.warning(f"Error analyzing patch {patch.get('version')}: {e}")
                continue
                
        return analyzed_patches
        
    def prepare_patch_content(self, patch: Dict) -> str:
        """Prepare patch content for GPT-4 analysis"""
        content_parts = [
            f"VALORANT Patch {patch['version']}",
            f"Release Date: {patch['release_date']}"
        ]
        
        if patch.get('patch_notes'):
            content_parts.append(f"Patch Notes: {patch['patch_notes']}")
            
        if patch.get('agent_changes'):
            content_parts.append(f"Agent Changes: {patch['agent_changes']}")
            
        if patch.get('map_changes'):
            content_parts.append(f"Map Changes: {patch['map_changes']}")
            
        if patch.get('weapon_changes'):
            content_parts.append(f"Weapon Changes: {patch['weapon_changes']}")
            
        return "\n\n".join(content_parts)
        
    async def generate_gpt4_analysis(self, patch_content: str) -> str:
        """Generate GPT-4 analysis of patch impact"""
        try:
            prompt = f"""
            You are an expert VALORANT esports analyst. Analyze the following patch notes and provide:

            1. A competitive impact score from 0.0 to 1.0 where:
               - 0.0-0.2: Minimal impact (bug fixes, minor tweaks)
               - 0.3-0.5: Moderate impact (balance changes, minor agent updates)
               - 0.6-0.8: High impact (significant agent reworks, map changes)
               - 0.9-1.0: Meta-defining (major system changes, new agents/maps)

            2. A concise summary (2-3 sentences) explaining the key changes and their competitive implications.

            Format your response as:
            IMPACT_SCORE: [0.0-1.0]
            SUMMARY: [Your analysis summary]

            Patch Content:
            {patch_content}
            """
            
            response = await self.call_openai_api(prompt)
            return response
            
        except Exception as e:
            logger.error(f"Error generating GPT-4 analysis: {e}")
            return "IMPACT_SCORE: 0.5\nSUMMARY: Unable to analyze patch impact."
            
    async def call_openai_api(self, prompt: str) -> str:
        """Call OpenAI API with proper error handling"""
        try:
            response = await openai.ChatCompletion.acreate(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are an expert VALORANT esports analyst."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=500,
                temperature=0.3
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.error(f"OpenAI API error: {e}")
            return "IMPACT_SCORE: 0.5\nSUMMARY: Error analyzing patch impact."
            
    def extract_impact_info(self, analysis: str) -> tuple[float, str]:
        """Extract impact score and summary from GPT-4 response"""
        try:
            lines = analysis.split('\n')
            impact_score = 0.5
            summary = "Patch analysis unavailable."
            
            for line in lines:
                if line.startswith('IMPACT_SCORE:'):
                    score_text = line.replace('IMPACT_SCORE:', '').strip()
                    try:
                        impact_score = float(score_text)
                        impact_score = max(0.0, min(1.0, impact_score))  # Clamp to [0,1]
                    except ValueError:
                        pass
                        
                elif line.startswith('SUMMARY:'):
                    summary = line.replace('SUMMARY:', '').strip()
                    
            return impact_score, summary
            
        except Exception as e:
            logger.warning(f"Error extracting impact info: {e}")
            return 0.5, "Error extracting patch analysis."
            
    def save_patches_to_bronze(self, patches: List[Dict]) -> int:
        """Save analyzed patches to bronze layer"""
        if not patches:
            return 0
            
        return self.save_to_bronze('bronze_patches', patches)
        
    async def update_matches_with_patch_info(self, patches: List[Dict]):
        """Update existing matches with patch information"""
        try:
            from ..database import db
            
            for patch in patches:
                patch_date = datetime.fromisoformat(patch['release_date'])
                
                # Update matches that started after this patch release
                update_query = """
                    UPDATE bronze_matches 
                    SET patch_version = ?, days_since_patch = CAST(julianday(start_time) - julianday(?) AS INTEGER)
                    WHERE start_time >= ? AND (patch_version IS NULL OR patch_version = '')
                """
                
                params = [
                    patch['version'],
                    patch['release_date'],
                    patch['release_date']
                ]
                
                db.execute_query(update_query, params)
                
        except Exception as e:
            logger.error(f"Error updating matches with patch info: {e}")
            
    async def analyze_recent_patches(self) -> Dict[str, Any]:
        """Public method to analyze recent patches"""
        async with self:
            return await self.ingest_data()