defmodule Docomo.Mixfile do
  use Mix.Project

  def project do
    [
      app: :docomo,
      version: "0.0.1",
      elixir: "~> 1.4",
      elixirc_paths: elixirc_paths(Mix.env),
      compilers: [:phoenix, :gettext] ++ Mix.compilers,
      start_permanent: Mix.env == :prod,
      deps: deps()
    ]
  end

  # Configuration for the OTP application.
  #
  # Type `mix help compile.app` for more information.
  def application do
    [
      mod: {Docomo.Application, []},
      extra_applications: [:logger, :runtime_tools]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_),     do: ["lib"]

  # Specifies your project dependencies.
  #
  # Type `mix help deps` for examples and options.
  defp deps do
    [
      {:phoenix, "~> 1.3.0"},
      {:phoenix_pubsub, "~> 1.0"},
      {:gettext, "~> 0.11"},
      {:cowboy, "~> 1.0"},
      {:amqp, "~> 0.2.0-pre.4"}, # https://github.com/pma/amqp/issues/28
      {:briefly, "~> 0.3"},
      {:ex_aws, "~> 1.0"},
      {:hackney, "~> 1.6"},
      {:poison, "~> 3.1"},
      {:redix, ">= 0.0.0"}
    ]
  end
end
