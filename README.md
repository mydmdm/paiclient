# The `python` SDK for `OpenPAI`

This is a proof-of-concept client (SDK) of `python` for the [OpenPAI](www.github.com/microsoft/pai). The SDK would be an independent package, which can be installed via `pip` inside the job container or in user local environment.

By providing a bag of APIs, the SDK can facilitate users access `OpenPAI` services or establish high level applications scenarios (e.g. for education).

## Services

### Core services

The SDK provide a class `OpenPAIClient` to let users access services. The client can be constructed by specifying the required information (in user local environment), or reading them via `os.environ` (easiest way inside job container).

- [ ] login (via token)
- [ ] submit a job
- [ ] list jobs and status

### Storage

The SDK provides generalized interface to access storage, supporting basic operations like `list`, `download`, `upload`. The API would know storage protocol and root information through the client or environment variables.

- `HDFS`
    - [x] list / download / upload

### Logging

The SDK may provide a logging service. Maybe it would hijack `logging` package to let user without modifying.

### `Jupyter` notebook based interactive mode

Interactive mode is an important feature that for users, now the solution is to start a long running job and connect it with `ssh` or better `jupyter` notebook. This is quite convenient and user can focus on his/her own codes and let `OpenPAI` to handle the environment problems. This solutions is suitable for the scenarios such as

- Education. Students follows the instructions step by step, to explore data, construct neural networks, train and evaluate.
- Debug mode. User may write or modify codes directly.

However, the concern is that GPUs may be idle when user prepare the codes (and it may take long time), and the resource cannot be used by others due to they are exclusively allocated to the long-running jobs.

Here we give a solution named **`resubmit`** method. User may prepare the codes through a notebook in local environment or a CPU-only job container (or with cheap GPUs). After complete the functionality of the codes, the SDK provide a method to (re-)submit the notebook to a real job environment with high performance GPUs.

To implement more complicated functions, the SDK can provide a method to let code know its running environment like local (outside `OpenPAI`), debug (interactive) job container or normal (batch) job container. With the environment information and / or task information like role, user can write only one copy of code, and let it runs partially according to environment.

### Deployment

As we know, the `OpenPAI` itself is providing a protocol based deployment method in [PR](). Here we talk this topic again is that it could be realized in the same way like `resubmit`. 

## Examples

### Notebook tutorials

- Submit from local notebook

