initSidebarItems({"enum":[["CoordinatorRPC","The list of supported RPC methods at the coordinator."],["Placement","Defines the placement of job workers on the available executors."],["PublishError","The error message sent back for failed publication request."],["SubmissionError","The error type for failed job submissions."],["SubscribeError","The error message sent back to unsuccessful subscription requests."],["TerminationError","The error type for failed job termination requests."],["UnpublishError","The error message sent back for failed unpublication request."],["UnsubscribeError","The error message sent back for failed unsubscription request."],["WorkerGroupError","The error cause sent back to worker groups when job spawning fails."]],"mod":[["catalog","This module contains the queries which can be submitted to the catalog service to inspect the current state of the system."]],"struct":[["AddExecutor","The message sent by new executors to register themselves at the coordinator."],["AddWorkerGroup","Registers a newly spawned worker group at the coordinator."],["ExecutorError","Error which occurs when coordinator rejects the registration of a new executor."],["JobToken","An opaque token used by job worker groups to authenticate themselves at the coordinator."],["Lookup","Looks up a topic at the coordinator without registering a subscription for it."],["Publish","A request to publish a topic."],["Submission","A new job submission."],["Subscribe","A topic subscription request, sent by a a spawned job the the coordinator."],["Termination","A job termination request."],["Unpublish","A request to unpublish a published topic."],["Unsubscribe","A request to unsubscribe from a topic."]]});