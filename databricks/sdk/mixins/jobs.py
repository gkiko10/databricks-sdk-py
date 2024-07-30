from typing import Optional

from ..service.jobs import JobsAPI
from databricks.sdk.service.jobs import Run


class JobsExt(JobsAPI):
    def get_run(self,
                run_id: int,
                *,
                include_history: Optional[bool] = None,
                include_resolved_values: Optional[bool] = None,
                page_token: Optional[str] = None) -> Run:
        """Get a single job run.

        Retrieve the metadata of a run.

        :param run_id: int
          The canonical identifier of the run for which to retrieve the metadata. This field is required.
        :param include_history: bool (optional)
          Whether to include the repair history in the response.
        :param include_resolved_values: bool (optional)
          Whether to include resolved parameter values in the response.
        :param page_token: str (optional)
          TODO(JOBS-16923) make this field PUBLIC [API 2.2]: use `next_page_token` or `prev_page_token`
          returned from the previous request to list the next or previous page of GetRun's sub-resources.

        :returns: :class:`Run`
        """

        query = {}
        if include_history is not None: query['include_history'] = include_history
        if include_resolved_values is not None: query['include_resolved_values'] = include_resolved_values
        if page_token is not None: query['page_token'] = page_token
        if run_id is not None: query['run_id'] = run_id
        headers = {'Accept': 'application/json', }

        res = self._api.do('GET', '/api/2.2/jobs/runs/get', query=query, headers=headers)

        run: Run = Run.from_dict(res)
        paginating_iterations = bool(run.iterations)
        page_token2 = run.next_page_token
        while page_token2 is not None:
            query['page_token'] = page_token2
            res = self._api.do('GET', '/api/2.2/jobs/runs/get', query=query, headers=headers)
            one_page_run: Run = Run.from_dict(res)
            if paginating_iterations:
                run.iterations.extend(one_page_run.iterations)
            else:
                run.tasks.extend(one_page_run.tasks)

            page_token2 = one_page_run.next_page_token

        return run
