from typing import Any

from dify_plugin import ToolProvider
from dify_plugin.errors.tool import ToolProviderCredentialValidationError


class KzVoiceToTextProvider(ToolProvider):
    def _validate_credentials(self, credentials: dict[str, Any]) -> None:
        try:
            print(credentials)

            if not credentials:
                return
            if not credentials.get("access_id"):
                raise ToolProviderCredentialValidationError("API key is required")
            if not credentials.get("access_secret"):
                raise ToolProviderCredentialValidationError("API secret is required")
            
        except Exception as e:
            raise ToolProviderCredentialValidationError(str(e))
