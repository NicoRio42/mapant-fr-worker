# Mapant.fr worker

A worker for the distributed computation of the [mapant.fr](mapant.fr) map.

## Installation and setup

Install the [Cassini topographic map generation software](https://cassini-map.com/guides/installation-and-setup/) globally.

Install the [Deno Javascript runtime](https://docs.deno.com/runtime/getting_started/installation/).

Install Mapant.fr worker globally:

```sh
deno install -g -RWEN --allow-run --allow-ffi --node-modules-dir=auto --allow-scripts=npm:sharp --env-file -n mapant-fr-worker jsr:@nicorio/mapant-fr-worker
```

Create a `.env` file (see `.env.example`) and set values for `MAPANT_API_WORKER_ID` and `MAPANT_API_TOKEN` environment varriables. These values are provided by the mapant.fr project admin (contact@mapant.fr).

## Usage

Open a terminal in the directory containing your `.env` file, and launch `mapant-fr-worker`. You can specify the number of parallel threads with the `--threads` flag:

```sh
mapant-fr-worker --threads 12
```
