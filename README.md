# pmtiles_tool

A simple tool to work with mbtiles and pmtiles archives.

This tool was created because the Python tool uses a lot of memory and takes a long time
to process large tilesets. This tool is optimized for memory and uses multiple threads
to decompress tiles.

Currently implemented subcommands:

- [x] convert (convert mbtiles to pmtiles)
- [x] info (show statistics of a pmtiles archive)
- [x] serve (serve tiles from a pmtiles archive)

Run `pmtiles_tool help` for more information:

```
$ pmtiles_tool help
pmtiles_tool
A tool for working with pmtiles archives

USAGE:
    pmtiles_tool <SUBCOMMAND>

OPTIONS:
    -h, --help    Print help information

SUBCOMMANDS:
    convert    Convert a mbtiles archive to a pmtiles archive
    help       Print this message or the help of the given subcommand(s)
    info       Get information about a pmtiles archive
    serve      Serve XYZ tiles from a pmtiles archive
```

Each subcommand has its own help page:

```
$ pmtiles_tool help convert
pmtiles_tool-convert
Convert a mbtiles archive to a pmtiles archive

USAGE:
    pmtiles_tool convert <INPUT> <OUTPUT>

ARGS:
    <INPUT>     Input
    <OUTPUT>    Output

OPTIONS:
    -h, --help    Print help information
```
