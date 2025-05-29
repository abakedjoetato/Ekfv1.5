"""
Emerald's Killfeed - Player Connection Lifecycle Parser
Complete rebuild implementing 4-event lifecycle tracking with live counts
"""

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Set
import discord
from bot.utils.embed_factory import EmbedFactory

logger = logging.getLogger(__name__)

class PlayerState:
    """Represents the current state of a player in the connection lifecycle"""
    
    def __init__(self, player_id: str, player_name: str = None):
        self.player_id = player_id
        self.player_name = player_name
        self.current_state = 'OFFLINE'  # OFFLINE, QUEUED, JOINED, DISCONNECTED
        self.last_event_time = datetime.now(timezone.utc)
        self.state_history = []
        self.can_queue = True
        self.can_join = False
        self.can_leave = False
        
    def can_transition_to(self, new_state: str) -> bool:
        """Check if player can transition to the new state"""
        valid_transitions = {
            'OFFLINE': ['QUEUED'],
            'QUEUED': ['JOINED', 'DISCONNECTED'],
            'JOINED': ['DISCONNECTED'],
            'DISCONNECTED': ['QUEUED']  # Allow re-queueing after disconnect
        }
        
        return new_state in valid_transitions.get(self.current_state, [])
    
    def transition_to(self, new_state: str, event_type: str) -> bool:
        """Attempt to transition to new state, returns True if successful"""
        if not self.can_transition_to(new_state):
            logger.warning(f"Invalid state transition for {self.player_id}: {self.current_state} -> {new_state}")
            return False
            
        # Record the transition
        self.state_history.append({
            'from_state': self.current_state,
            'to_state': new_state,
            'event_type': event_type,
            'timestamp': datetime.now(timezone.utc)
        })
        
        self.current_state = new_state
        self.last_event_time = datetime.now(timezone.utc)
        
        # Update capability flags
        self._update_capabilities()
        
        logger.debug(f"Player {self.player_id} transitioned to {new_state} via {event_type}")
        return True
    
    def _update_capabilities(self):
        """Update what actions the player can perform based on current state"""
        if self.current_state == 'OFFLINE':
            self.can_queue = True
            self.can_join = False
            self.can_leave = False
        elif self.current_state == 'QUEUED':
            self.can_queue = False
            self.can_join = True
            self.can_leave = True  # Can disconnect from queue
        elif self.current_state == 'JOINED':
            self.can_queue = False
            self.can_join = False
            self.can_leave = True
        elif self.current_state == 'DISCONNECTED':
            self.can_queue = True
            self.can_join = False
            self.can_leave = False
    
    def is_duplicate_event(self, event_type: str, time_threshold: float = 60.0) -> bool:
        """Check if this event is a duplicate within the time threshold"""
        current_time = datetime.now(timezone.utc)
        time_diff = (current_time - self.last_event_time).total_seconds()
        
        # If same event type happened recently, it's likely a duplicate
        if (self.state_history and 
            self.state_history[-1]['event_type'] == event_type and 
            time_diff < time_threshold):
            return True
            
        return False

class ConnectionLifecycleParser:
    """
    PLAYER CONNECTION + COUNT SYSTEM - Complete Rebuild
    Tracks 4 core lifecycle events: jq, j2, d1, d2
    Maintains live counts: QC = jq - j2 - d2, PC = j2 - d1
    """

    def __init__(self, bot):
        self.bot = bot
        
        # Live count tracking per server
        self.server_counts: Dict[str, Dict[str, Any]] = {}
        
        # Individual player state machine tracking per server
        # server_key -> player_id -> PlayerState
        self.player_states: Dict[str, Dict[str, 'PlayerState']] = {}
        
        # Player ID to name mapping cache per server
        self.player_names: Dict[str, Dict[str, str]] = {}
        
        # Track recent connection messages to prevent duplicates (server_key -> player_id -> timestamp)
        self.recent_connections: Dict[str, Dict[str, float]] = {}
        self.recent_disconnections: Dict[str, Dict[str, float]] = {}
        
        # Compile robust regex patterns for the 4 lifecycle events
        self.lifecycle_patterns = {
            # 1. Queue Join (jq) - Player enters queue
            'queue_join': re.compile(
                r'LogNet: Join request: /Game/Maps/world_0/World_0\?.*\?Name=([^&\s]+).*(?:platformid=PS5:(\w+)|eosid=\|(\w+))', 
                re.IGNORECASE
            ),
            
            # 2. Player Joined (j2) - Player successfully registered
            'player_joined': re.compile(
                r'LogOnline: Warning: Player \|(\w+) successfully registered!', 
                re.IGNORECASE
            ),
            
            # 3. Disconnect Post-Join (d1) - Standard disconnect after joining
            'disconnect_post_join': re.compile(
                r'UChannel::Close: Sending CloseBunch.*UniqueId: EOS:\|(\w+)', 
                re.IGNORECASE
            ),
            
            # 4. Disconnect Pre-Join (d2) - Disconnect from queue before joining
            'disconnect_pre_join': re.compile(
                r'UChannel::Close: Sending CloseBunch.*UniqueId: (?:PS5|EOS):\|?(\w+)', 
                re.IGNORECASE
            )
        }

    def initialize_server_tracking(self, server_key: str):
        """Initialize tracking structures for a server"""
        if server_key not in self.server_counts:
            self.server_counts[server_key] = {
                'queue_count': 0,  # QC = players in QUEUED state
                'player_count': 0,  # PC = players in JOINED state
            }
            
        if server_key not in self.player_states:
            self.player_states[server_key] = {}
            
        if server_key not in self.player_names:
            self.player_names[server_key] = {}
            
        if server_key not in self.recent_connections:
            self.recent_connections[server_key] = {}
            
        if server_key not in self.recent_disconnections:
            self.recent_disconnections[server_key] = {}

    async def parse_lifecycle_event(self, line: str, server_key: str, guild_id: int) -> Optional[Dict[str, Any]]:
        """Parse a single log line for lifecycle events with intelligent state tracking"""
        self.initialize_server_tracking(server_key)
        
        # Extract player ID from the line for tracking
        player_id = None
        event_type = None
        player_name = None
        
        # Check for Queue Join (jq)
        if match := self.lifecycle_patterns['queue_join'].search(line):
            player_name = match.group(1)
            player_id = match.group(2) or match.group(3)  # PS5 or EOS ID
            event_type = 'queue_join'
            
            if player_id:
                # Get or create player state
                player_state = self._get_or_create_player_state(server_key, player_id, player_name)
                
                # Check for duplicate or invalid transition
                if player_state.is_duplicate_event(event_type):
                    logger.debug(f"Ignoring duplicate queue join for {player_id}")
                    return None
                
                # Attempt state transition
                if player_state.transition_to('QUEUED', event_type):
                    await self._update_live_counts(server_key)
                    logger.info(f"🟡 Queue Join: {player_name or player_id} joined queue")
                else:
                    logger.warning(f"Invalid queue join attempt for {player_id} in state {player_state.current_state}")
        
        # Check for Player Joined (j2)
        elif match := self.lifecycle_patterns['player_joined'].search(line):
            player_id = match.group(1)
            event_type = 'player_joined'
            
            # Get or create player state
            player_state = self._get_or_create_player_state(server_key, player_id)
            
            # Try to extract player name from the current line or previous context
            extracted_name = self._extract_player_name_from_log_line(line, player_id)
            if extracted_name:
                player_state.player_name = extracted_name
                await self._cache_player_name(server_key, player_id, extracted_name)
            
            # Check for duplicate or invalid transition
            if player_state.is_duplicate_event(event_type):
                logger.debug(f"Ignoring duplicate player join for {player_id}")
                return None
            
            # Attempt state transition
            if player_state.transition_to('JOINED', event_type):
                await self._update_live_counts(server_key)
                logger.info(f"🟢 Player Joined: {player_state.player_name or player_id} successfully registered")
                
                # Create join embed with resolved name
                return await self._create_join_embed(player_id, player_state.player_name, server_key)
            else:
                logger.warning(f"Invalid join attempt for {player_id} in state {player_state.current_state}")
        
        # Check for Disconnect (both pre and post join)
        elif match := self.lifecycle_patterns['disconnect_post_join'].search(line) or \
             self.lifecycle_patterns['disconnect_pre_join'].search(line):
            player_id = match.group(1)
            event_type = 'disconnect'
            
            # Get player state if it exists
            if server_key in self.player_states and player_id in self.player_states[server_key]:
                player_state = self.player_states[server_key][player_id]
                
                # Check for duplicate disconnect
                if player_state.is_duplicate_event(event_type):
                    logger.debug(f"Ignoring duplicate disconnect for {player_id}")
                    return None
                
                # Determine if this should create an embed based on current state
                should_create_embed = player_state.current_state == 'JOINED'
                
                # Attempt state transition
                if player_state.transition_to('DISCONNECTED', event_type):
                    await self._update_live_counts(server_key)
                    
                    if should_create_embed:
                        logger.info(f"🔴 Player Left: {player_state.player_name or player_id} disconnected")
                        return await self._create_leave_embed(player_id, player_state.player_name, server_key)
                    else:
                        logger.info(f"🟠 Queue Disconnect: {player_state.player_name or player_id} left queue")
                else:
                    logger.warning(f"Invalid disconnect attempt for {player_id} in state {player_state.current_state}")
        
        return None

    def _get_or_create_player_state(self, server_key: str, player_id: str, player_name: str = None) -> PlayerState:
        """Get existing player state or create a new one"""
        if player_id not in self.player_states[server_key]:
            self.player_states[server_key][player_id] = PlayerState(player_id, player_name)
        elif player_name and not self.player_states[server_key][player_id].player_name:
            # Update player name if we have it and it's not set
            self.player_states[server_key][player_id].player_name = player_name
        
        return self.player_states[server_key][player_id]

    async def _update_live_counts(self, server_key: str):
        """Update live player and queue counts based on current player states"""
        player_states = self.player_states[server_key]
        counts = self.server_counts[server_key]
        
        # Count players by their current state
        queue_count = 0
        player_count = 0
        
        for player_id, player_state in player_states.items():
            if player_state.current_state == 'QUEUED':
                queue_count += 1
            elif player_state.current_state == 'JOINED':
                player_count += 1
        
        counts['queue_count'] = queue_count
        counts['player_count'] = player_count
        
        logger.info(f"📊 Live Counts - Players: {player_count}, Queue: {queue_count}")
        
        # Update voice channels with new counts
        await self._update_voice_channels(server_key, player_count, queue_count)

    async def _update_voice_channels(self, server_key: str, player_count: int, queue_count: int):
        """Update voice channel names with current player and queue counts"""
        try:
            # Extract guild_id and server_id from server_key (format: guild_id_server_id)
            parts = server_key.split('_')
            guild_id = int(parts[0])
            server_id = parts[1] if len(parts) > 1 else 'unknown'
            
            guild = self.bot.get_guild(guild_id)
            if not guild:
                return

            # Get guild configuration to find the configured playercountvc channel
            if not hasattr(self.bot, 'db_manager') or not self.bot.db_manager:
                logger.warning("Bot database not available for voice channel update")
                return

            guild_config = await self.bot.db_manager.get_guild(guild_id)
            if not guild_config:
                return

            # Look for playercountvc channel (set by /setchannel playercountvc command)
            channels = guild_config.get('channels', {})
            voice_channel_id = channels.get('playercountvc')

            if not voice_channel_id:
                logger.debug(f"No playercountvc channel configured for guild {guild_id}")
                return

            voice_channel = guild.get_channel(voice_channel_id)
            if not voice_channel:
                logger.warning(f"Voice channel {voice_channel_id} not found for guild {guild_id}")
                return

            # Get server name from guild config
            servers = guild_config.get('servers', [])
            server_name = 'Unknown Server'
            for server_config in servers:
                if str(server_config.get('_id', '')) == server_id:
                    server_name = server_config.get('name', f'Server {server_id}')
                    break

            # Format: "📈 ServerName: players/max (queue in queue)" 
            max_players = 50  # Default, should be updated from server config
            if queue_count > 0:
                new_name = f"📈 {server_name}: {player_count}/{max_players} ({queue_count} in queue)"
            else:
                new_name = f"📈 {server_name}: {player_count}/{max_players}"

            # Update channel name if different
            if voice_channel.name != new_name:
                await voice_channel.edit(name=new_name)
                logger.info(f"🔊 Updated voice channel: {new_name}")
                        
        except Exception as e:
            logger.error(f"Failed to update voice channels: {e}")

    async def _create_join_embed(self, player_id: str, player_name: Optional[str] = None, server_key: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Create themed embed for player join event"""
        # Check for duplicate within last minute
        if self._is_duplicate_connection(server_key, player_id, 'join'):
            logger.debug(f"Blocking duplicate join message for {player_id}")
            return None
            
        # Resolve player name if not provided - NEVER show player ID
        resolved_name = await self._resolve_player_name(player_id, server_key)
        display_name = resolved_name or player_name
        
        # If we still don't have a name, don't send the embed
        if not display_name or display_name == player_id:
            logger.warning(f"No player name resolved for ID {player_id}, skipping embed")
            return None
        
        embed_data = {
            'connection_id': display_name,
            'timestamp': datetime.now(timezone.utc)
        }
        
        embed, file_attachment = await EmbedFactory.build('player_join', embed_data)
        result = {'type': 'player_connection', 'embed': embed, 'file': file_attachment}
        
        # Mark as sent to prevent duplicates
        self._mark_connection_sent(server_key, player_id, 'join')
        
        # Send to connections channel if configured
        if server_key:
            await self._send_connection_embed(result, server_key)
        
        return result

    async def _create_leave_embed(self, player_id: str, player_name: Optional[str] = None, server_key: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Create themed embed for player leave event"""
        # Check for duplicate within last minute
        if self._is_duplicate_connection(server_key, player_id, 'leave'):
            logger.debug(f"Blocking duplicate leave message for {player_id}")
            return None
            
        # Resolve player name if not provided - NEVER show player ID
        resolved_name = await self._resolve_player_name(player_id, server_key)
        display_name = resolved_name or player_name
        
        # If we still don't have a name, don't send the embed
        if not display_name or display_name == player_id:
            logger.warning(f"No player name resolved for ID {player_id}, skipping embed")
            return None
        
        embed_data = {
            'connection_id': display_name,
            'timestamp': datetime.now(timezone.utc)
        }
        
        embed, file_attachment = await EmbedFactory.build('player_leave', embed_data)
        result = {'type': 'player_disconnection', 'embed': embed, 'file': file_attachment}
        
        # Mark as sent to prevent duplicates
        self._mark_connection_sent(server_key, player_id, 'leave')
        
        # Send to connections channel if configured
        if server_key:
            await self._send_connection_embed(result, server_key)
        
        return result

    async def _send_connection_embed(self, embed_result: Dict[str, Any], server_key: str):
        """Send connection embed to the correct connections channel"""
        try:
            # Extract guild_id from server_key
            guild_id = int(server_key.split('_')[0])
            
            # Get guild configuration
            if not hasattr(self.bot, 'db_manager') or not self.bot.db_manager:
                logger.warning("Bot database not available for sending connection embeds")
                return

            guild_config = await self.bot.db_manager.get_guild(guild_id)
            if not guild_config:
                return

            # Look for connections channel (set by /setchannel connections command)
            channels = guild_config.get('channels', {})
            connections_channel_id = channels.get('connections')

            if not connections_channel_id:
                logger.debug(f"No connections channel configured for guild {guild_id}")
                return

            channel = self.bot.get_channel(connections_channel_id)
            if not channel:
                logger.warning(f"Connections channel {connections_channel_id} not found for guild {guild_id}")
                return

            # Send the embed
            embed = embed_result.get('embed')
            file_attachment = embed_result.get('file')
            
            if embed:
                if file_attachment:
                    await channel.send(embed=embed, file=file_attachment)
                else:
                    await channel.send(embed=embed)
                logger.debug(f"Sent connection embed to connections channel: {channel.name}")

        except Exception as e:
            logger.error(f"Failed to send connection embed: {e}")

    def get_live_counts(self, server_key: str) -> Dict[str, int]:
        """Get current live player and queue counts for a server"""
        self.initialize_server_tracking(server_key)
        return self.server_counts[server_key].copy()

    def reset_server_counts(self, server_key: str):
        """Reset all counts for a server (useful for log rotation)"""
        if server_key in self.server_counts:
            del self.server_counts[server_key]
        if server_key in self.player_states:
            del self.player_states[server_key]
        if server_key in self.player_names:
            del self.player_names[server_key]
        logger.info(f"🔄 Reset player counts for server {server_key}")

    async def _resolve_player_name(self, player_id: str, server_key: str) -> Optional[str]:
        """Resolve player ID to player name using database lookup and caching"""
        try:
            # Check cache first
            if server_key in self.player_names and player_id in self.player_names[server_key]:
                return self.player_names[server_key][player_id]
            
            # Extract guild_id and server_id from server_key
            parts = server_key.split('_')
            guild_id = int(parts[0])
            server_id = parts[1] if len(parts) > 1 else 'unknown'
            
            # Search for player name in recent kill events
            if hasattr(self.bot, 'db_manager') and self.bot.db_manager:
                # Look for recent kills/deaths involving this player ID
                recent_kills = await self.bot.db_manager.get_recent_kills(guild_id, server_id, limit=100)
                
                for kill_event in recent_kills:
                    # Check if this player ID appears as killer
                    if kill_event.get('killer_id') == player_id and kill_event.get('killer'):
                        player_name = kill_event.get('killer')
                        await self._cache_player_name(server_key, player_id, player_name)
                        return player_name
                    
                    # Check if this player ID appears as victim
                    if kill_event.get('victim_id') == player_id and kill_event.get('victim'):
                        player_name = kill_event.get('victim')
                        await self._cache_player_name(server_key, player_id, player_name)
                        return player_name
                
                # Search in PvP data for linked characters
                players_cursor = self.bot.db_manager.players.find({'guild_id': guild_id})
                async for player_doc in players_cursor:
                    # Check if this player ID matches any character names (approximate match)
                    linked_chars = player_doc.get('linked_characters', [])
                    for char_name in linked_chars:
                        # Simple check if player_id is contained in character name or vice versa
                        if player_id.lower() in char_name.lower() or char_name.lower() in player_id.lower():
                            await self._cache_player_name(server_key, player_id, char_name)
                            return char_name
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to resolve player name for ID {player_id}: {e}")
            return None

    async def _cache_player_name(self, server_key: str, player_id: str, player_name: str):
        """Cache player ID to name mapping"""
        if server_key not in self.player_names:
            self.player_names[server_key] = {}
        self.player_names[server_key][player_id] = player_name
        logger.debug(f"Cached player name: {player_id} -> {player_name}")

    def _extract_player_name_from_log_line(self, line: str, player_id: str) -> Optional[str]:
        """Extract player name from log lines that contain the player ID"""
        try:
            # Look for patterns that contain both the player ID and a potential name
            # Pattern: "Player Name (ID)" or "Name|ID" or similar
            import re
            
            # Try to find name patterns near the player ID
            patterns = [
                rf'Name=([^&\s]+).*{re.escape(player_id)}',  # Name= parameter before ID
                rf'{re.escape(player_id)}.*Name=([^&\s]+)',  # ID before Name= parameter
                rf'Player\s+([^|]+)\|{re.escape(player_id)}',  # Player Name|ID format
                rf'([A-Za-z][A-Za-z0-9_\s]+)\s*\({re.escape(player_id)}\)',  # Name (ID) format
            ]
            
            for pattern in patterns:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    name = match.group(1).strip()
                    # Validate that it looks like a player name
                    if len(name) > 1 and not name.isdigit():
                        return name
            
            return None
            
        except Exception as e:
            logger.debug(f"Failed to extract player name from log line: {e}")
            return None

    def _is_duplicate_connection(self, server_key: str, player_id: str, event_type: str) -> bool:
        """Check if this connection event is a duplicate within the last minute"""
        import time
        current_time = time.time()
        
        if event_type == 'join':
            recent_dict = self.recent_connections
        else:
            recent_dict = self.recent_disconnections
            
        if server_key not in recent_dict:
            recent_dict[server_key] = {}
            
        # Check if this player had a recent event
        if player_id in recent_dict[server_key]:
            last_time = recent_dict[server_key][player_id]
            if current_time - last_time < 60:  # Within 1 minute
                return True
                
        return False
        
    def _mark_connection_sent(self, server_key: str, player_id: str, event_type: str):
        """Mark a connection event as sent to prevent duplicates"""
        import time
        current_time = time.time()
        
        if event_type == 'join':
            recent_dict = self.recent_connections
        else:
            recent_dict = self.recent_disconnections
            
        if server_key not in recent_dict:
            recent_dict[server_key] = {}
            
        recent_dict[server_key][player_id] = current_time
        
        # Clean up old entries (older than 2 minutes)
        to_remove = []
        for pid, timestamp in recent_dict[server_key].items():
            if current_time - timestamp > 120:
                to_remove.append(pid)
        for pid in to_remove:
            del recent_dict[server_key][pid]