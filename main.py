from __future__ import annotations
import subprocess
import logging
from datetime import datetime, date
from functools import lru_cache
from typing import Union, Literal

import glob
import json
import re
import yaml
import pandas as pd

from pathlib import Path

log = logging.getLogger("mkdocs")

# ─── Constants & Pre-Compiled Regex ───────────────────────────────────────────

BRANCH_PLACEHOLDER = "{branch_name}"
GITHUB_BASE = "http://github.com/TIDES-transit/TIDES/tree/"
GITHUB_REPO = f"{GITHUB_BASE}{BRANCH_PLACEHOLDER}"

UPDATE_LINKS: dict[str, str] = {
    "[architecture]": "architecture.md",
    "[change-policy]": "../governance/policies/change-management",
    # …etc…
    "[`tides.spec.json`]": f"{GITHUB_REPO}/spec/tides.spec.json",
}

MD_LINK_DEF_REGEX = re.compile(r"^(\[[^\]]+\]):\s*(.+)$", re.MULTILINE)
MD_HEADING_REGEX  = re.compile(r"^(#{1,6})\s+(.*)", re.MULTILINE)

# ─── Utility Functions ────────────────────────────────────────────────────────

def split_yaml_header(text: str, delimiter: str = "---") -> tuple[dict, str]:
    """Extract YAML front-matter and the body from a markdown file."""
    parts = text.split(delimiter, 2)
    if len(parts) != 3:
        raise ValueError("Expected text with YAML front-matter delimited by '---'")
    _, raw_yaml, body = parts
    return yaml.safe_load(raw_yaml), body

@lru_cache(maxsize=1)
def get_git_branch_name() -> str:
    """Return current Git branch, caching result for the session."""
    branch = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True, text=True, check=True
    ).stdout.strip()
    log.info(f"On branch: {branch}")
    return branch

def downshift_md_heading(md: str) -> str:
    """Increase all headings by one level (e.g. # → ##), up to six."""
    def repl(m: re.Match) -> str:
        hashes, title = m.group(1), m.group(2)
        return f"{'#' if len(hashes) < 6 else ''}{hashes} {title}"
    return MD_HEADING_REGEX.sub(repl, md)

def replace_links_in_markdown(content: str, links: dict[str, str] = UPDATE_LINKS) -> str:
    """Update markdown link-definitions based on UPDATE_LINKS map."""
    def repl(m: re.Match) -> str:
        key = m.group(1)
        return f"{key}: {links[key]}" if key in links else m.group(0)
    return MD_LINK_DEF_REGEX.sub(repl, content)

def list_to_file_list(
    inputs: Union[str, list[str]], 
    glob_pattern: str = "*"
) -> list[str]:
    """Given a file or directory (or list thereof), return all matching file paths."""
    paths = []
    for item in ([inputs] if isinstance(inputs, str) else inputs):
        p = Path(item)
        if p.is_file():
            paths.append(str(p))
        elif p.is_dir():
            for f in p.glob(glob_pattern):
                if f.is_file():
                    paths.append(str(f))
        else:
            log.warning(f"Skipping unknown path: {item}")
    return paths

# ─── MkDocs Hook ──────────────────────────────────────────────────────────────

def define_env(env) -> None:
    """Register MkDocs macros to the environment."""

    @env.macro
    def list_actions(files: Union[str, list[str]]) -> str:
        """Render a reverse-chronological list of action items from markdown files."""
        filepaths = list_to_file_list(files, glob_pattern="*.md")
        actions: list[dict] = []
        for fp in filepaths:
            raw = Path(fp).read_text(encoding="utf-8")
            meta, md_txt = split_yaml_header(raw)
            meta["md_txt"] = md_txt
            actions.append(meta)

        actions.sort(
            key=lambda x: x["date"] 
                      if isinstance(x["date"], date) 
                      else datetime.strptime(x["date"], "%Y-%m-%d"),
            reverse=True
        )

        entries = []
        for act in actions:
            title = f'"{act["date"]} {act["title"]}"'
            lines = [f"??? abstract {title}\n"]
            if via := act.get("via"):
                lines.append(f"    :material-file-check: {via}")
            if loc := act.get("loc"):
                lines.append(f"    :material-folder-open: [full document]({loc})")
            # indent body
            body = "\n    ".join(act["md_txt"].splitlines())
            lines.append(f"    {body}")
            entries.append("\n".join(lines))

        return "\n\n".join(entries)

    @env.macro
    def url_for(filepath: str) -> str:
        """Convert a source path to its MkDocs URL path."""
        p = Path(filepath)
        rel = p.as_posix().removeprefix("docs/")
        url = f"/{rel[:-3]}/" if rel.endswith(".md") else f"/{rel}"
        return url

    @env.macro
    def include_file(
        filename: str,
        downshift_h1: bool = True,
        start_line: int = 0,
        end_line: int | None = None,
        code_type: str | None = None
    ) -> str:
        """Include part (or all) of a file into the doc, optionally fenced as code."""
        full = Path(env.project_dir) / filename
        lines = full.read_text(encoding="utf-8").splitlines()
        snippet = "\n".join(lines[start_line:end_line])
        if downshift_h1:
            snippet = downshift_md_heading(snippet)
        snippet = replace_links_in_markdown(snippet)
        if BRANCH_PLACEHOLDER in snippet:
            snippet = snippet.replace(BRANCH_PLACEHOLDER, get_git_branch_name())
        if code_type:
            return f"```{code_type} title='{filename}'\n{snippet}\n```"
        return snippet

    @env.macro
    def include_file_sections(
        filename: str,
        include_sections: list[str] = [],
        exclude_sections: list[str] = [],
        **kwargs
    ) -> str:
        """
        Include only specific headings from a file. 
        `include_sections` and `exclude_sections` are case-insensitive lists of titles.
        """
        content = include_file(filename, **kwargs)
        link_defs = MD_LINK_DEF_REGEX.findall(content)
        parts = re.split(MD_HEADING_REGEX, content)[1:]  # drop preamble
        # parts come in triples: (hashes, title, body)
        sections = [
            (lvl, title.strip(), body.strip())
            for lvl, title, body in zip(parts[0::3], parts[1::3], parts[2::3])
        ]
        inc = {s.lower() for s in include_sections}
        exc = {s.lower() for s in exclude_sections}
        if inc & exc:
            conflict = inc & exc
            raise ValueError(f"Cannot both include and exclude: {conflict}")

        out = []
        for lvl, title, body in sections:
            tkey = title.lower()
            if (not inc or tkey in inc) and tkey not in exc:
                out.append(f"{lvl} {title}\n\n{body}")

        # re-append link definitions:
        for lbl, href in link_defs:
            out.append(f"{lbl}: {href}")
        return "\n\n".join(out)

    @env.macro
    def frictionless_data_package(
        data_package_path: str = "**/data-package.json",
        sub_schema: str | None = None,
        include: Literal["required","recommended","all"] = "recommended"
    ) -> str:
        """
        Generate a markdown table of top-level fields from a data-package.json.
        """
        dp_file = glob.glob(data_package_path, recursive=True)[0]
        log.info(f"Documenting {dp_file}, sub-schema={sub_schema}, include={include}")
        with open(dp_file, "r", encoding="utf-8") as f:
            dp = json.load(f)

        # drill into sub_schema if needed
        if sub_schema:
            dp = (dp.get("properties", {}) or dp.get("$defs", {})).get(sub_schema, dp)
            if dp.get("type") == "array":
                dp = dp["items"]

        # pick fields
        props = dp.get("properties", {})
        required = set(dp.get("required", []))
        recommended = set(dp.get("recommended", []))
        if include == "required":
            field_names = list(required)
        elif include == "recommended":
            field_names = list(required | recommended)
        else:
            field_names = list(props)

        rows = []
        for name in field_names:
            prop = props[name]
            desc = prop.get("description", "")
            if enum := prop.get("enum"):
                vals = "</li><li>".join(map(str, enum))
                desc += f"<br>**Must be one of:**<ul><li>{vals}</li></ul>"
            example = prop.get("examples")
            if example:
                desc += f"<br>**Example:** `{example[0]}`"
            rows.append({
                "name": f"`{name}`",
                "description": desc,
                "type": prop.get("type",""),
                "requirement": "required" if name in required else
                               "recommended" if name in recommended else "-"
            })

        df = pd.DataFrame(rows, columns=["name","description","type","requirement"])
        return df.to_markdown(index=False)

    # …and so on for the other macros (frictionless_spec, frictionless_schemas, etc.)…
