from abc import ABC


class PipelineEvent(ABC):
    def __init__(self, pipeline_id: str, update_id: str, message: str):
        self.pipeline_id = pipeline_id
        self.update_id = update_id
        self.message = message

    def __str__(self) -> str:
        return (
            f"Pipeline(pipeline-id={self.pipeline_id}, update-id={self.update_id}) - {self.message}"
        )


class PipelineRefresh(PipelineEvent):
    def __init__(self, pipeline_id: str, update_id: str, state: str):
        super().__init__(pipeline_id, update_id, f"Refreshing - got state {state}")


class PipelineRefreshError(PipelineEvent):
    def __init__(self, pipeline_id: str, update_id: str, message: str):
        super().__init__(pipeline_id, update_id, f"Error refreshing pipeline: {message}")
