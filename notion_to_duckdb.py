import dlt
from rest_api import RESTAPIConfig, rest_api_source

from dlt.sources.helpers.rest_client.paginators import BasePaginator, JSONResponsePaginator
from dlt.sources.helpers.requests import Response, Request


class PostBodyPaginator(BasePaginator):
    def __init__(self):
        super().__init__()
        self.cursor = None

    def update_state(self, response: Response) -> None:
        # Assuming the API returns an empty list when no more data is available
        if not response.json():
            self._has_next_page = False
        else:
            self.cursor = response.json().get("next_cursor")
            if self.cursor is None:
                self._has_next_page = False

    def update_request(self, request: Request) -> None:
        if request.json is None:
            request.json = {}

        # Add the cursor to the request body
        request.json["start_cursor"] = self.cursor


def rest_api_notion_source():
    notion_config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.notion.com/v1/",
            "auth": {
                "token": dlt.secrets["sources.rest_api.notion.api_key"]
            },
            "headers":{
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
            }
        },
        "resources": [
            {
                "name": "search",
                "endpoint": {
                    "path": "search",
                    "method": "POST",
                    "paginator": PostBodyPaginator(),
                    "json": {
                        "query": "",
                        "sort": {
                            "direction": "ascending",
                            "timestamp": "last_edited_time"
                        }
                    },
                    "data_selector": "results"
                }
            },
            {
                "name": "page_content",
                "endpoint": {
                    "path": "blocks/{page_id}/children",
                    "paginator": JSONResponsePaginator(),
                    "params": {
                        "page_id": {
                            "type": "resolve",
                            "resource": "search",
                            "field": "id"
                        }
                    },
                }
            }
        ]
    }

    yield from rest_api_source(notion_config)



def load_notion() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="notion_to_duckdb",
        destination="duckdb",
        dataset_name="notion_pages"
    )

    load_info = pipeline.run(rest_api_notion_source)
    print(load_info)

if __name__ == '__main__':
    load_notion()
