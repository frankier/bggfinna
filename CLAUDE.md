# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Purpose

The purpose of this repository is to grab all the boad games available in a particular region of Finnish libraries, given as input. Information from Board Game Geek (BGG) is then used to obtain ranks. Then we can see the top ranked games we can get from the library, and also filter by other stuff like "family" and "co-op"

First all information and the relevant fields is dumped into a CSV from Finna.

A second script is then going to get extra information from BGG.

Then there's some joining and sorting.

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

Tools:
 
 * Python
 * uv
 * requests
 * DuckDB
