"""
Encryption Utilities
====================

Utility functions for handling encryption and decryption of configuration values.

Improvement over original: Fernet instances are cached after first load so the
key file is read from disk only once per key path. This is safe for config
decryption at startup and avoids repeated disk I/O if called multiple times.
"""

import logging
from functools import lru_cache
from pathlib import Path
from cryptography.fernet import Fernet, InvalidToken


logger = logging.getLogger(__name__)


class EncryptionError(Exception):
    """Custom exception for encryption-related errors."""
    pass


class EncryptionUtility:
    """Utility class for handling encryption operations."""

    @staticmethod
    def generate_key() -> bytes:
        """Generate a new Fernet encryption key."""
        return Fernet.generate_key()

    @staticmethod
    def save_key(key: bytes, filepath: str) -> None:
        """
        Save an encryption key to file with restrictive permissions.

        Args:
            key: The Fernet encryption key bytes.
            filepath: Destination path for the key file.

        Raises:
            EncryptionError: If the key cannot be saved.
        """
        try:
            key_path = Path(filepath)
            key_path.parent.mkdir(parents=True, exist_ok=True)

            with open(key_path, "wb") as f:
                f.write(key)

            try:
                key_path.chmod(0o600)
            except (OSError, AttributeError):
                pass  # Windows or systems without chmod

            logger.info(f"Encryption key saved to: {filepath}")

        except Exception as e:
            raise EncryptionError(f"Failed to save encryption key to {filepath}: {e}")

    @staticmethod
    def load_key(filepath: str) -> bytes:
        """
        Load and validate a Fernet key from file.

        Args:
            filepath: Path to the key file.

        Returns:
            bytes: The raw key bytes.

        Raises:
            EncryptionError: If the key file is missing or invalid.
        """
        try:
            key_path = Path(filepath)

            if not key_path.exists():
                raise EncryptionError(f"Encryption key file not found: {filepath}")

            if not key_path.is_file():
                raise EncryptionError(f"Encryption key path is not a file: {filepath}")

            with open(key_path, "rb") as f:
                key = f.read()

            try:
                Fernet(key)  # validate format
            except ValueError as e:
                raise EncryptionError(f"Invalid encryption key format in {filepath}: {e}")

            return key

        except EncryptionError:
            raise
        except Exception as e:
            raise EncryptionError(f"Failed to load encryption key from {filepath}: {e}")

    @staticmethod
    @lru_cache(maxsize=16)
    def _get_fernet(key_path: str) -> Fernet:
        """
        Return a cached Fernet instance for the given key path.

        The key is loaded from disk only on the first call for a given path.
        Subsequent calls return the cached instance, avoiding repeated disk I/O.

        Args:
            key_path: Path to the encryption key file.

        Returns:
            Fernet: A ready-to-use Fernet instance.
        """
        key = EncryptionUtility.load_key(key_path)
        logger.debug(f"Loaded and cached Fernet instance for key: {key_path}")
        return Fernet(key)

    @staticmethod
    def encrypt_value(value: str, key_path: str) -> str:
        """
        Encrypt a string value using the key at key_path.

        Args:
            value: The plaintext string to encrypt.
            key_path: Path to the Fernet key file.

        Returns:
            str: Base64-encoded encrypted string.

        Raises:
            EncryptionError: If encryption fails.
        """
        try:
            if not value:
                raise EncryptionError("Cannot encrypt empty value")

            fernet = EncryptionUtility._get_fernet(key_path)
            encrypted_bytes = fernet.encrypt(value.encode("utf-8"))
            return encrypted_bytes.decode("utf-8")

        except EncryptionError:
            raise
        except Exception as e:
            raise EncryptionError(f"Failed to encrypt value: {e}")

    @staticmethod
    def decrypt_value(encrypted_value: str, key_path: str) -> str:
        """
        Decrypt a base64-encoded encrypted string using the key at key_path.

        Args:
            encrypted_value: The encrypted string (base64-encoded).
            key_path: Path to the Fernet key file.

        Returns:
            str: The decrypted plaintext string.

        Raises:
            EncryptionError: If decryption fails or the token is invalid.
        """
        try:
            if not encrypted_value:
                raise EncryptionError("Cannot decrypt empty value")

            fernet = EncryptionUtility._get_fernet(key_path)

            try:
                decrypted_bytes = fernet.decrypt(encrypted_value.encode("utf-8"))
                return decrypted_bytes.decode("utf-8")
            except InvalidToken:
                raise EncryptionError("Invalid encrypted value or wrong decryption key")

        except EncryptionError:
            raise
        except Exception as e:
            raise EncryptionError(f"Failed to decrypt value: {e}")

    @staticmethod
    def validate_key_file(key_path: str) -> bool:
        """
        Validate that a key file exists and contains a valid Fernet key.

        Args:
            key_path: Path to the key file.

        Returns:
            bool: True if valid, False otherwise.
        """
        try:
            EncryptionUtility.load_key(key_path)
            return True
        except Exception:
            return False
