"""
Emerald's Killfeed - Intelligent Connection Lifecycle Parser
Advanced state machine that prevents duplicate events and tracks logical player transitions
"""

import asyncio
import re
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Set
import discord
from bot.utils.embed_factory import EmbedFactory

logger = logging.getLogger(__name__)

class PlayerConnectionState:
    """Tracks individual player connection state with intelligent duplicate prevention"""

    def __init__(self, player_id: str, player_name: str = None):
        self.player_id = player_id
        self.player_name = player_name
        self.current_state = 'OFFLINE'  # OFFLINE, QUEUED, JOINED, DISCONNECTED
        self.last_event_time = datetime.now(timezone.utc)
        self.last_event_type = None
        self.state_transitions = []

    def can_transition_to(self, new_state: str) -> bool:
        """Check if player can logically transition to the new state"""
        valid_transitions = {
            'OFFLINE': ['QUEUED', 'CONNECTING', 'JOINED'],  # Allow direct join if queue event was missed
            'QUEUED': ['CONNECTING', 'JOINED', 'DISCONNECTED'],  # Added CONNECTING state
            'CONNECTING': ['JOINED', 'DISCONNECTED'],  # Beacon/intermediate state
            'JOINED': ['DISCONNECTED'],
            'DISCONNECTED': ['QUEUED', 'CONNECTING', 'JOINED', 'OFFLINE']  # Allow re-queueing or direct join after disconnect
        }

        return new_state in valid_transitions.get(self.current_state, [])

    def is_duplicate_event(self, event_type: str, time_threshold: float = 30.0) -> bool:
        """Check if this is a duplicate event within time threshold"""
        current_time = datetime.now(timezone.utc)
        time_diff = (current_time - self.last_event_time).total_seconds()

        # If same event type happened very recently, it's likely a duplicate
        if self.last_event_type == event_type and time_diff < time_threshold:
            return True

        return False

    def transition_to(self, new_state: str, event_type: str) -> bool:
        """Attempt to transition to new state, returns True if successful"""
        current_time = datetime.now(timezone.utc)

        # Check for duplicate first
        if self.is_duplicate_event(event_type):
            logger.debug(f"Ignoring duplicate {event_type} for {self.player_id}")
            return False

        # Check if transition is valid
        if not self.can_transition_to(new_state):
            logger.warning(f"Invalid transition for {self.player_id}: {self.current_state} -> {new_state} via {event_type}")
            return False

        # Record the successful transition
        self.state_transitions.append({
            'from_state': self.current_state,
            'to_state': new_state,
            'event_type': event_type,
            'timestamp': current_time
        })

        self.current_state = new_state
        self.last_event_time = current_time
        self.last_event_type = event_type

        logger.debug(f"Player {self.player_id} transitioned: {self.state_transitions[-2]['from_state'] if len(self.state_transitions) > 1 else 'OFFLINE'} -> {new_state}")
        return True

class IntelligentConnectionParser:
    """
    Intelligent parser that tracks player connection lifecycle with duplicate prevention
    """

    def __init__(self, bot):
        self.bot = bot

        # Player state tracking per server
        self.player_states: Dict[str, Dict[str, PlayerConnectionState]] = {}

        # Live count tracking per server
        self.server_counts: Dict[str, Dict[str, int]] = {}

        # Player name cache per server
        self.player_names: Dict[str, Dict[str, str]] = {}

        self.patterns = {
            # Core connection lifecycle patterns (matching actual log format)

            # 1. Queue Join - Player enters queue
            'queue_join': re.compile(r'LogNet: Join request: /Game/Maps/world_\d+/World_\d+\?.*Name=([^&\?]+).*eosid=\|([a-f0-9]+)', re.IGNORECASE),

            # 2. Beacon Join - Intermediate connection step
            'beacon_join': re.compile(r'LogBeacon: Beacon Join SFPSOnlineBeaconClient EOS:\|([a-f0-9]+)', re.IGNORECASE),

            # 3. Player Successfully Registered (actual join completion)
            'player_joined': re.compile(r'LogOnline: Warning: Player \|([a-f0-9]+) successfully registered!', re.IGNORECASE),

            # 4. Disconnect patterns (covers both pre and post join)
            'disconnect': re.compile(r'UChannel::Close: Sending CloseBunch.*UniqueId: EOS:\|([a-f0-9]+)', re.IGNORECASE),
            'disconnect_alt': re.compile(r'UNetConnection::Close:.*UniqueId: EOS:\|([a-f0-9]+)', re.IGNORECASE),

            # 5. Beacon disconnect pattern (was missing)
            'beacon_disconnect': re.compile(r'LogBeacon:.*Beacon.*(?:disconnect|close|cleanup).*EOS:\|([a-f0-9]+)', re.IGNORECASE),

            # 6. Mission events - Updated to match actual log format (no timestamp brackets)
            'mission_respawn': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]*_[Mm]is[_0-9]*) will respawn in (\d+)', re.IGNORECASE),
            'mission_initial': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]*_[Mm]is[_0-9]*) switched to INITIAL', re.IGNORECASE),
            'mission_ready': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]*_[Mm]is[_0-9]*) switched to READY', re.IGNORECASE),
            'mission_in_progress': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]*_[Mm]is[_0-9]*) switched to IN_PROGRESS', re.IGNORECASE),
            'mission_completed': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]*_[Mm]is[_0-9]*) switched to COMPLETED', re.IGNORECASE),

            # 7. Vehicle events - Updated to match actual log format
            'vehicle_spawn': re.compile(r'LogSFPS: \[ASFPSGameMode::NewVehicle_Add\] Add vehicle (BP_SFPSVehicle_[A-Za-z0-9_]+_C_\d+) Total (\d+)', re.IGNORECASE),
            'vehicle_delete': re.compile(r'LogSFPS: \[ASFPSGameMode::NewVehicle_Del\] Del vehicle (BP_SFPSVehicle_[A-Za-z0-9_]+_C_\d+) Total (\d+)', re.IGNORECASE),

            # 8. Alternate patterns for broader coverage
            'queue_join_alt': re.compile(r'LogNet: Join request:.*Name=([^&\s]+).*(?:platformid=(?:PS5|XSX|PC):(\w+)|eosid=\|(\w+))', re.IGNORECASE),

            # 9. Additional connection events for better coverage
            'player_connected': re.compile(r'LogOnline:.*Player.*(\w{32}).*connected', re.IGNORECASE),
            'network_close': re.compile(r'UChannel::Close.*UniqueId:.*(\w{32})', re.IGNORECASE),
            
            # 10. Server configuration patterns
            'server_max_players': re.compile(r'LogSFPS:.*playersmaxcount=(\d+)', re.IGNORECASE)
        }

    def initialize_server_tracking(self, server_key: str):
        """Initialize tracking structures for a server"""
        if server_key not in self.player_states:
            self.player_states[server_key] = {}

        if server_key not in self.server_counts:
            self.server_counts[server_key] = {
                'queue_count': 0,
                'player_count': 0
            }

        if server_key not in self.player_names:
            self.player_names[server_key] = {}

    def get_or_create_player_state(self, server_key: str, player_id: str, player_name: str = None) -> PlayerConnectionState:
        """Get existing player state or create a new one"""
        if player_id not in self.player_states[server_key]:
            self.player_states[server_key][player_id] = PlayerConnectionState(player_id, player_name)
        elif player_name and not self.player_states[server_key][player_id].player_name:
            self.player_states[server_key][player_id].player_name = player_name

        return self.player_states[server_key][player_id]

    async def parse_connection_event(self, line: str, server_key: str, guild_id: int) -> Optional[Dict[str, Any]]:
        """Parse a log line for connection events with intelligent duplicate prevention"""
        self.initialize_server_tracking(server_key)

        # Debug: Check each pattern individually and log what we're trying to match
        line_lower = line.lower()

        # Check for queue join
        queue_match = self.patterns['queue_join'].search(line)
        if queue_match:
            player_name = queue_match.group(1)
            player_id = queue_match.group(2)

            logger.debug(f"🔍 Queue join pattern matched: name={player_name}, id={player_id}")

            if player_id:
                player_state = self.get_or_create_player_state(server_key, player_id, player_name)

                if player_state.transition_to('QUEUED', 'queue_join'):
                    await self._update_counts(server_key)
                    logger.info(f"🟡 Queue Join: {player_name} ({player_id}) - Queue: {self.server_counts[server_key]['queue_count']}, Players: {self.server_counts[server_key]['player_count']}")

        # Check for beacon join (intermediate connection step)
        elif (match := self.patterns['beacon_join'].search(line)):
            player_id = match.group(1)
            logger.debug(f"🔍 Beacon join pattern matched: id={player_id}")

            if player_id:
                player_state = self.get_or_create_player_state(server_key, player_id)

                if player_state.transition_to('CONNECTING', 'beacon_join'):
                    logger.debug(f"🔗 Beacon Connect: {player_id} - transitioning to CONNECTING")

        # Check for player successfully registered (actual join completion)
        elif (match := self.patterns['player_joined'].search(line)):
            player_id = match.group(1)

            logger.debug(f"🔍 Player joined pattern matched: id={player_id}")

            # Find existing player state (they should have queued first)
            if player_id in self.player_states[server_key]:
                player_state = self.player_states[server_key][player_id]

                if player_state.transition_to('JOINED', 'player_joined'):
                    await self._update_counts(server_key)
                    logger.info(f"🟢 Player Joined: {player_state.player_name or player_id} - Queue: {self.server_counts[server_key]['queue_count']}, Players: {self.server_counts[server_key]['player_count']}")
                    await self._queue_join_embed(player_state, server_key, guild_id)
                else:
                    logger.debug(f"Player {player_id} join event ignored (already joined or invalid transition)")
            else:
                # Player joined without queueing - create state and allow direct join
                player_state = self.get_or_create_player_state(server_key, player_id)
                # Force transition to JOINED even from OFFLINE (missing queue event)
                player_state.current_state = 'OFFLINE'  # Ensure we start from OFFLINE
                if player_state.transition_to('JOINED', 'player_joined'):
                    await self._update_counts(server_key)
                    logger.info(f"🟢 Direct Join (missed queue): {player_id} - Queue: {self.server_counts[server_key]['queue_count']}, Players: {self.server_counts[server_key]['player_count']}")
                    await self._queue_join_embed(player_state, server_key, guild_id)

        # Check for disconnect (try both patterns)
        elif (match := self.patterns['disconnect'].search(line)) or (match := self.patterns['disconnect_alt'].search(line)):
            player_id = match.group(1)

            logger.debug(f"🔍 Disconnect pattern matched: id={player_id}")

            if player_id in self.player_states[server_key]:
                player_state = self.player_states[server_key][player_id]

                # Only create leave embed if player was actually joined
                should_create_embed = player_state.current_state == 'JOINED'

                if player_state.transition_to('DISCONNECTED', 'disconnect'):
                    await self._update_counts(server_key)

                    if should_create_embed:
                        logger.info(f"🔴 Player Left: {player_state.player_name or player_id} - Queue: {self.server_counts[server_key]['queue_count']}, Players: {self.server_counts[server_key]['player_count']}")
                        await self._queue_leave_embed(player_state, server_key, guild_id)
                    else:
                        logger.info(f"🟠 Queue Disconnect: {player_state.player_name or player_id} - Queue: {self.server_counts[server_key]['queue_count']}, Players: {self.server_counts[server_key]['player_count']}")
                else:
                    logger.debug(f"Player {player_id} disconnect event ignored (invalid transition or already disconnected)")
            else:
                # Player disconnect without any prior state - create minimal state for tracking
                player_state = self.get_or_create_player_state(server_key, player_id)
                # Don't create embed for unknown disconnects, just log
                logger.debug(f"🟠 Unknown Player Disconnect: {player_id}")

        # Check for beacon disconnect
        elif (match := self.patterns['beacon_disconnect'].search(line)):
            player_id = match.group(1) if match.groups() else None
            logger.debug(f"🔍 Beacon disconnect detected: id={player_id}")
            
            if player_id and player_id in self.player_states[server_key]:
                player_state = self.player_states[server_key][player_id]
                
                # Only create disconnect embed if player was actually connected/joined
                should_create_embed = player_state.current_state == 'JOINED'
                
                if player_state.transition_to('DISCONNECTED', 'beacon_disconnect'):
                    await self._update_counts(server_key)
                    
                    if should_create_embed:
                        logger.info(f"🔴 Beacon Disconnect (was joined): {player_state.player_name or player_id}")
                        await self._queue_leave_embed(player_state, server_key, guild_id)
                    else:
                        logger.info(f"🟠 Beacon Disconnect (from queue/connecting): {player_state.player_name or player_id}")

        # Check for server max players configuration
        elif (match := self.patterns['server_max_players'].search(line)):
            max_players = int(match.group(1))
            logger.info(f"🎯 Server max players detected: {max_players}")
            await self._update_server_max_players(server_key, max_players)
        else:
            # Debug: Log lines that contain player-related keywords but don't match patterns
            if any(keyword in line_lower for keyword in ['player', 'join', 'request', 'registered', 'uniqueid', 'uchannel', 'close']):
                logger.debug(f"🔍 Player-related line not matched: {line[:100]}...")

        return None

    async def _update_counts(self, server_key: str):
        """Update live player and queue counts"""
        # Ensure server is initialized before accessing
        self.initialize_server_tracking(server_key)

        queue_count = 0
        player_count = 0

        for player_id, player_state in self.player_states[server_key].items():
            if player_state.current_state == 'QUEUED':
                queue_count += 1
            elif player_state.current_state == 'JOINED':
                player_count += 1

        self.server_counts[server_key]['queue_count'] = queue_count
        self.server_counts[server_key]['player_count'] = player_count

        logger.info(f"📊 Live Counts - Players: {player_count}, Queue: {queue_count}")
        await self._update_voice_channels(server_key, player_count, queue_count)

    async def _update_voice_channels(self, server_key: str, player_count: int, queue_count: int):
        """Update voice channel names with current counts"""
        try:
            parts = server_key.split('_')
            guild_id = int(parts[0])
            server_id = parts[1] if len(parts) > 1 else 'unknown'

            guild = self.bot.get_guild(guild_id)
            if not guild or not hasattr(self.bot, 'db_manager') or not self.bot.db_manager:
                return

            guild_config = await self.bot.db_manager.get_guild(guild_id)
            if not guild_config:
                return

            channels = guild_config.get('channels', {})
            voice_channel_id = channels.get('playercountvc')

            if not voice_channel_id:
                return

            voice_channel = guild.get_channel(voice_channel_id)
            if not voice_channel:
                return

            # Get server name
            servers = guild_config.get('servers', [])
            server_name = f'Server {server_id}'
            for server_config in servers:
                if str(server_config.get('_id', '')) == server_id:
                    server_name = server_config.get('name', server_name)
                    break

            # Format channel name - use stored max players or default
            max_players = self.server_counts[server_key].get('max_players', 50)
            if queue_count > 0:
                new_name = f"📈 {server_name}: {player_count}/{max_players} ({queue_count} in queue)"
            else:
                new_name = f"📈 {server_name}: {player_count}/{max_players}"

            if voice_channel.name != new_name:
                await voice_channel.edit(name=new_name)
                logger.info(f"🔊 Updated voice channel: {new_name}")

        except Exception as e:
            logger.error(f"Failed to update voice channels: {e}")

    async def _queue_join_embed(self, player_state: PlayerConnectionState, server_key: str, guild_id: int):
        """Queue join embed for player using batch sender"""
        if not player_state.player_name:
            logger.warning(f"No player name for join embed: {player_state.player_id}")
            return

        try:
            # Get connections channel
            guild_config = await self.bot.db_manager.get_guild(guild_id)
            if not guild_config:
                return

            channels = guild_config.get('channels', {})
            connections_channel_id = channels.get('connections')

            if not connections_channel_id:
                logger.debug(f"No connections channel configured for guild {guild_id}")
                return

            embed_data = {
                'connection_id': player_state.player_name,
                'timestamp': datetime.now(timezone.utc)
            }

            embed, file_attachment = await EmbedFactory.build('player_join', embed_data)

            await self.bot.batch_sender.queue_embed(
                channel_id=connections_channel_id,
                embed=embed,
                file=file_attachment
            )

        except Exception as e:
            logger.error(f"Failed to queue join embed: {e}")

    async def _queue_leave_embed(self, player_state: PlayerConnectionState, server_key: str, guild_id: int):
        """Queue leave embed for player using batch sender"""
        if not player_state.player_name:
            logger.warning(f"No player name for leave embed: {player_state.player_id}")
            return

        try:
            # Get connections channel
            guild_config = await self.bot.db_manager.get_guild(guild_id)
            if not guild_config:
                return

            channels = guild_config.get('channels', {})
            connections_channel_id = channels.get('connections')

            if not connections_channel_id:
                logger.debug(f"No connections channel configured for guild {guild_id}")
                return

            embed_data = {
                'connection_id': player_state.player_name,
                'timestamp': datetime.now(timezone.utc)
            }

            embed, file_attachment = await EmbedFactory.build('player_leave', embed_data)

            await self.bot.batch_sender.queue_embed(
                channel_id=connections_channel_id,
                embed=embed,
                file=file_attachment
            )

        except Exception as e:
            logger.error(f"Failed to queue leave embed: {e}")

    def get_server_stats(self, server_key: str) -> Dict[str, Any]:
        """Get current server statistics"""
        if server_key not in self.server_counts:
            return {'queue_count': 0, 'player_count': 0}

        return self.server_counts[server_key].copy()

    def cleanup_old_states(self, max_age_hours: int = 24):
        """Clean up old player states to prevent memory leaks"""
        cutoff_time = datetime.now(timezone.utc).timestamp() - (max_age_hours * 3600)

        for server_key in list(self.player_states.keys()):
            for player_id in list(self.player_states[server_key].keys()):
                player_state = self.player_states[server_key][player_id]
                if (player_state.current_state == 'DISCONNECTED' and 
                    player_state.last_event_time.timestamp() < cutoff_time):
                    del self.player_states[server_key][player_id]
                    logger.debug(f"Cleaned up old state for {player_id}")

    def reset_server_counts(self, server_key: str):
        """Reset counts for a specific server"""
        if server_key in self.player_states:
            self.player_states[server_key].clear()
        if server_key in self.server_counts:
            self.server_counts[server_key] = {'queue_count': 0, 'player_count': 0}
        logger.info(f"🔄 Reset player counts for server {server_key}")

    def debug_server_state(self, server_key: str):
        """Debug current server state"""
        if server_key not in self.player_states:
            logger.info(f"🐛 No state data for server {server_key}")
            return

        states = {}
        for player_id, player_state in self.player_states[server_key].items():
            state = player_state.current_state
            if state not in states:
                states[state] = []
            states[state].append(f"{player_state.player_name or player_id}")

        counts = self.server_counts.get(server_key, {'queue_count': 0, 'player_count': 0})
        logger.info(f"🐛 Server {server_key} Debug - Counts: Queue={counts['queue_count']}, Players={counts['player_count']}")
        for state, players in states.items():
            logger.info(f"🐛 {state}: {players}")

    def verify_regex_patterns(self, sample_log_lines: list = None) -> dict:
        """Verify regex patterns against sample log lines"""
        if sample_log_lines is None:
            # Use actual log lines from the attached Deadside.log file
            sample_log_lines = [
                "LogNet: Join request: /Game/Maps/world_0/World_0?logintype=eos&login=Njshh&Name=Njshh&eosid=|0002e69a65204b669c20238266782d7b",
                "LogBeacon: Beacon Join SFPSOnlineBeaconClient EOS:|0002e69a65204b669c20238266782d7b",
                "LogOnline: Warning: Player |0002e69a65204b669c20238266782d7b successfully registered!",
                "UChannel::Close: Sending CloseBunch. ChIndex == 0. UniqueId: EOS:|0002e69a65204b669c20238266782d7b",
                "UNetConnection::Close: Connection cleanup. UniqueId: EOS:|0002e69a65204b669c20238266782d7b",
                "LogSFPS: Mission GA_Settle_05_ChernyLog_mis1 will respawn in 221",
                "LogSFPS: Mission GA_Settle_05_ChernyLog_mis1 switched to INITIAL",
                "LogSFPS: Mission GA_Military_02_mis1 switched to READY",
                "LogSFPS: [ASFPSGameMode::NewVehicle_Add] Add vehicle BP_SFPSVehicle_Ural_M6736_Sidecar_C_2147482394 Total 1",
                "LogSFPS: [ASFPSGameMode::NewVehicle_Del] Del vehicle BP_SFPSVehicle_Ural_M6736_Sidecar_C_2147482394 Total 0"
            ]

        results = {}
        for pattern_name, pattern in self.patterns.items():
            matches = []
            for line in sample_log_lines:
                match = pattern.search(line)
                if match:
                    matches.append({
                        'line': line,
                        'groups': match.groups(),
                        'groupdict': match.groupdict() if hasattr(match, 'groupdict') else {}
                    })
            results[pattern_name] = {
                'match_count': len(matches),
                'matches': matches
            }

        logger.info(f"Pattern verification results: {json.dumps({k: v['match_count'] for k, v in results.items()}, indent=2)}")
        return results

    async def _update_server_max_players(self, server_key: str, max_players: int):
        """Update server max players and refresh voice channel"""
        # Ensure server is initialized
        self.initialize_server_tracking(server_key)
        
        # Store max players in server counts
        if 'max_players' not in self.server_counts[server_key]:
            self.server_counts[server_key]['max_players'] = max_players
        else:
            self.server_counts[server_key]['max_players'] = max_players
        
        logger.info(f"🎯 Updated max players for {server_key}: {max_players}")
        
        # Update voice channel with new max player count
        player_count = self.server_counts[server_key]['player_count']
        queue_count = self.server_counts[server_key]['queue_count']
        await self._update_voice_channels(server_key, player_count, queue_count)

    def test_counting_logic(self, server_key: str) -> dict:
        """Test the mathematical soundness of counting logic"""
        if server_key not in self.player_states:
            return {'error': 'No player states found'}

        player_states = self.player_states[server_key]

        # Count by state
        state_counts = {}
        queue_count = 0
        player_count = 0

        for player_id, player_state in player_states.items():
            state = player_state.current_state
            state_counts[state] = state_counts.get(state, 0) + 1

            if state == 'QUEUED':
                queue_count += 1
            elif state == 'JOINED':
                player_count += 1

        # Verify against get_server_stats
        official_stats = self.get_server_stats(server_key)

        return {
            'manual_count': {
                'queue_count': queue_count,
                'player_count': player_count,
                'state_breakdown': state_counts
            },
            'official_stats': official_stats,
            'discrepancy': {
                'queue_diff': official_stats.get('queue_count', 0) - queue_count,
                'player_diff': official_stats.get('player_count', 0) - player_count
            },
            'total_tracked': len(player_states)
        }