defmodule NeurytEventStoreBitcask.Mixfile do
  use Mix.Project

  def project do
    [app: :neuryt_event_store_bitcask,
     version: "0.1.0",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [applications: [:logger, :bitcask, :neuryt],
     # mod: {NeurytEventStoreBitcask.Application, []}
    ]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    [
      {:bitcask, "~> 2.0"},
      {:neuryt, path: "../neuryt"},
      {:dialyxir, "~> 0.3.5", only: [:test, :dev]},
      {:excoveralls, "~> 0.5", only: :test},
    ]
  end
end
