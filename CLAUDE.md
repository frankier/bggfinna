## Purpose

The purpose of this repository is to grab all the boad games available in a particular region of Finnish libraries, given as input. Information from Board Game Geek (BGG) is then used to obtain ranks. Then we can see the top ranked games we can get from the library, and also filter by other stuff like "family" and "co-op"

This is done using a pipeline which ends up as a DuckDB database.

A Streamlit dashboard allows the user to browse the database.

## Coding style

 * Be succinct!
 * PEP-8 where possible.
 * Don't swallow unknown exceptions or write generic error handlers, just let the default error bubble up.
 * Don't make programs output too much and be too chatty.
 * Try to make code speak for itself, but short docstrings and occasional comments are welcome.

## Background

 * Finna
   * Finna API docs entrypoint: https://www.kiwi.fi/spaces/Finna/pages/53839221/Finna+API+in+English
   * Find further information through web search and looking through those pages
 * BGG
   * There is an API documented at https://boardgamegeek.com/wiki/page/BGG_XML_API2

## Current Configuration

- Claude Code settings are configured in `.claude/settings.local.json`
- Current permissions allow bash `ls` commands

## Development Setup

Tools and libraries in use include
 
 * Python (run with `. "$HOME/.cargo/env" && uv run python`)
 * uv
 * requests
 * DuckDB
 * Streamlit

You might need to add `$HOME/.cargo/bin` to your `$PATH` to run uv.

Streamlit does not show errors when with the `streamlit` command so always try running the script with `python` first.

## Usage

Refer to @README.md
