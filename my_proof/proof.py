import json
import logging
import os
from typing import Dict, Any, List
import hashlib
import hmac
from urllib.parse import parse_qsl
from datetime import datetime
from my_proof.filebase_service import FilebaseService

from my_proof.models.proof_response import ProofResponse


class Proof:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.proof_response = ProofResponse(dlp_id=config['dlp_id'])

    def generate(self) -> ProofResponse:
        """Generate proofs for all input files."""
        logging.info("Starting proof generation")
        input_data = None

        # Process all files as potential JSON files
        for input_filename in os.listdir(self.config['input_dir']):
            input_file = os.path.join(self.config['input_dir'], input_filename)
            try:
                with open(input_file, 'r') as f:
                    input_data = json.load(f)
                    break  # Stop after first successful JSON parse
            except json.JSONDecodeError:
                logging.warning(f"File {input_filename} is not a valid JSON file, skipping")
                continue
            except Exception as e:
                logging.error(f"Error reading file {input_filename}: {str(e)}")
                raise

        if input_data is None:
            raise ValueError("No valid JSON files found in input directory")

        self.proof_response.uniqueness = self.calc_uniqueness(input_data)
        self.proof_response.ownership = self.calc_ownership(input_data)
        self.proof_response.authenticity = self.calc_authenticity(input_data)

        quality_data = self.calc_quality(input_data)

        self.proof_response.quality = quality_data['quality_score']

        # Calculate overall score and validity
        self.proof_response.score = round(0.6 * self.proof_response.quality + 0.4 * self.proof_response.ownership, 3)
        self.proof_response.valid = self.proof_response.uniqueness == 1.0

        self.proof_response.attributes = {
            'stats': quality_data['stats'],
            'messages_count': quality_data['stats']['total_messages'],
            'component_scores': quality_data['component_scores'],
            'character_slug': input_data.get("character_slug", ''),
            'character_level': input_data.get("character_level", '')
        }

        # Additional metadata about the proof, written onchain
        self.proof_response.metadata = {
            'dlp_id': self.config['dlp_id'],
            'type': input_data.get("type", ''),
            'allow_reuse': self.config['allow_reuse']
        }

        return self.proof_response


    def calc_uniqueness(self, input: List[Dict[str, Any]]) -> float:
        filebase_service = FilebaseService(
            filebase_access_key_id=self.config['filebase_access_key_id'], filebase_secret_access_key=self.config['filebase_secret_access_key']
        )

        generated_hash = filebase_service.generate_hash(input)
        existing_hashes = filebase_service.get_hash_list()

        if generated_hash in existing_hashes:
            return 0.0
        else:
            filebase_service.update_hash_list(existing_hashes, generated_hash)
            return 1.0
        
    def calc_quality(self, input: List[Dict[str, Any]]) -> Dict[str, Any]:
        stats = {
            "total_messages": len(input.get("messages", [])),
            "empty_messages": 0,
            "invalid_dates": 0,
            "large_gaps": 0,  # gaps > 24h
            "replies": 0
        }
        
        prev_date = None
        
        # Analyze messages
        for msg in input.get("messages", []):
            # Content check
            if not msg.get("text"):
                stats["empty_messages"] += 1
                
            # Reply check    
            if msg.get("reply_to_message_id"):
                stats["replies"] += 1
                
            # Time check
            try:
                current_date = datetime.strptime(msg["date"], "%Y-%m-%dT%H:%M:%S")
                if prev_date and (current_date - prev_date).total_seconds() > 86400:
                    stats["large_gaps"] += 1
                prev_date = current_date
            except Exception:
                stats["invalid_dates"] += 1

        

        # Calculate quality scores (0-1 for each component)
        if stats["total_messages"] == 0:
            scores = {
                "content": 0,
                "time": 0,
                "interaction": 0
            }
        else:
            scores = {
                # Content quality: ratio of non-empty messages
                "content": 1 - (stats["empty_messages"] / stats["total_messages"]),
                
                # Time quality: penalize gaps and invalid dates
                "time": 1 - min(1, (stats["large_gaps"] * 0.1 + 
                                stats["invalid_dates"] / stats["total_messages"])),
                
                # Interaction quality: ratio of replies
                "interaction": min(1, stats["replies"] / (stats["total_messages"] * 0.3))
            }
    
        # Overall quality score (weighted average)
        weights = {"content": 0.8, "time": 0.1, "interaction": 0.1}
        quality_score = sum(scores[k] * weights[k] for k in scores)
        
        return {
            "quality_score": round(quality_score, 3),
            "component_scores": {k: round(v, 3) for k, v in scores.items()},
            "stats": stats
        }

    def calc_authenticity(self, input: List[Dict[str, Any]]) -> float:
        chat_type = input.get("type", "")
        return 1.0 if chat_type in ["personal_chat", "ai_chat"] else 0.0

    def calc_ownership(self, input: List[Dict[str, Any]]) -> float:
        parsed_data = dict(parse_qsl(self.config['tg_init_data']))
        
        data_check_string = '\n'.join(
            f"{k}={v}" for k, v in sorted(parsed_data.items()) if k != 'hash'
        )

        secret_key = hmac.new("WebAppData".encode(), self.config['telegram_bot_access_key'].encode(), hashlib.sha256).digest()
        h = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256)
        
        # Verify hash and check Telegram ID
        if h.hexdigest() == parsed_data.get('hash', ''):
            user_info = parsed_data.get("user")
            if user_info:
                tg_id = json.loads(user_info).get("id")
                if str(tg_id) in str(input):
                    return 1.0
            return 0.0  # Return 0 if user_info is missing or tg_id doesn't match
        
        return 0.0  # Return 0 if hash verification fails
