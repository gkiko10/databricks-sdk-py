from typing import Optional

from databricks.sdk.service import jobs
from databricks.sdk.service.jobs import Job


class JobsExt(jobs.JobsAPI):

    def get_run(self,
                run_id: int,
                *,
                include_history: Optional[bool] = None,
                include_resolved_values: Optional[bool] = None,
                page_token: Optional[str] = None) -> jobs.Run:
        """
        This method fetches the details of a run identified by `run_id`. If the run has multiple pages of tasks or iterations,
        it will paginate through all pages and aggregate the results.
        :param run_id: int
          The canonical identifier of the run for which to retrieve the metadata. This field is required.
        :param include_history: bool (optional)
          Whether to include the repair history in the response.
        :param include_resolved_values: bool (optional)
          Whether to include resolved parameter values in the response.
        :param page_token: str (optional)
          To list the next page or the previous page of job tasks, set this field to the value of the
          `next_page_token` or `prev_page_token` returned in the GetJob response.
        :returns: :class:`Run`
        """
        run = super().get_run(run_id,
                              include_history=include_history,
                              include_resolved_values=include_resolved_values,
                              page_token=page_token)

        # When querying a Job run, a page token is returned when there are more than 100 tasks. No iterations are defined for a Job run. Therefore, the next page in the response only includes the next page of tasks.
        # When querying a ForEach task run, a page token is returned when there are more than 100 iterations. Only a single task is returned, corresponding to the ForEach task itself. Therefore, the client only reads the iterations from the next page and not the tasks.
        is_paginating_iterations = run.iterations is not None and len(run.iterations) > 0

        while run.next_page_token is not None:
            next_run = super().get_run(run_id,
                                       include_history=include_history,
                                       include_resolved_values=include_resolved_values,
                                       page_token=run.next_page_token)
            if is_paginating_iterations:
                run.iterations.extend(next_run.iterations)
            else:
                run.tasks.extend(next_run.tasks)
            run.next_page_token = next_run.next_page_token

        run.prev_page_token = None
        return run

    def get(self, job_id: int, *, page_token: Optional[str] = None) -> Job:
        """Get a single job.

        Retrieves the details for a single job. If the job has multiple pages of tasks, job_clusters, parameters or environments,
        it will paginate through all pages and aggregate the results.

        :param job_id: int
          The canonical identifier of the job to retrieve information about. This field is required.
        :param page_token: str (optional)
          Use `next_page_token` returned from the previous GetJob to request the next page of the job's
          sub-resources.

        :returns: :class:`Job`
        """
        job = super().get(job_id, page_token=page_token)

        # jobs/get response includes next_page_token as long as there are more pages to fetch.
        while job.next_page_token is not None:
            next_job = super().get(job_id, page_token=job.next_page_token)
            # Each new page of jobs/get response includes the next page of the tasks, job_clusters, job_parameters, and environments.
            job.settings.tasks.extend(next_job.settings.tasks)
            job.settings.job_clusters.extend(next_job.settings.job_clusters)
            job.settings.parameters.extend(next_job.settings.parameters)
            job.settings.environments.extend(next_job.settings.environments)
            job.next_page_token = next_job.next_page_token

        return job