## Augusta

Augusta is an asynchronous task schdeduler, it uses a lease based leader election and Kubernetes style task allocation. It runs light-weight tasks and it takes an agnostic approach to commands being ran.

Highlevel overview:

- Client adds a task using rest API
- Leader scheduler allocates tasks on available workers using a scheduling alogrithm(eg. roundrobin)
- Worker runs a task and reports the result to the leader
