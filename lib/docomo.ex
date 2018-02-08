defmodule Docomo do
  @moduledoc """
  Docomo keeps the contexts that define your domain
  and business logic.

  Contexts are also responsible for managing your data, regardless
  if it comes from the database, an external API or others.
  """

  #children = [
    # Start the endpoint when the application starts
    #supervisor(Docomo.Endpoint, []),
    #worker(Docomo.Worker, []),
  #]

  children = [
    # The Stack is a child started via Stack.start_link([:hello])
    %{
      id: Worker,
      start: {Docomo.Worker, :start_link, []}
    }
  ]

  # Now we start the supervisor with the children and a strategy
  {:ok, pid} = Supervisor.start_link(children, strategy: :one_for_one)

  # After started, we can query the supervisor for information
  #Supervisor.count_children(pid)
  #=> %{active: 1, specs: 1, supervisors: 0, workers: 1}
end
