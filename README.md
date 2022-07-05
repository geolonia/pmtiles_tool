# pmtiles_tool

A simple tool to work with mbtiles and pmtiles archives.

This tool was created because the Python tool uses a lot of memory and takes a long time
to process large tilesets. This tool is optimized for memory and uses multiple threads
to decompress tiles.

Currently implemented:

- [x] convert (convert mbtiles to pmtiles)
- [ ] info (show statistics of a pmtiles archive)
- [ ] serve (serve tiles from a pmtiles archive)
