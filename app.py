"""
S3 Connector — Streamlit entry point
--------------------------------------
A password-protected web application to browse and download files from an
S3-compatible storage bucket.

Users paste their S3 credentials directly in the sidebar UI. Credentials are
held in st.session_state for the duration of the browser session only and are
never written to disk.

Usage:
    streamlit run app.py

Environment variables:
    APP_PASSWORD    Plain-text password that protects the Streamlit interface
"""

import logging
from typing import Dict, List, Optional, Union

import streamlit as st
from dotenv import load_dotenv

from auth import render_login_page
from s3_client import S3Client, S3ConnectorError, S3Object

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("s3connector.app")

_BYTES_PER_KB = 1024
_BYTES_PER_MB = _BYTES_PER_KB ** 2
_BYTES_PER_GB = _BYTES_PER_KB ** 3


def _format_size(size_bytes: int) -> str:
    """Format a byte count as a human-readable string.

    Args:
        size_bytes: Raw byte count.

    Returns:
        Human-readable string such as "3.4 MB" or "512 B".
    """
    if size_bytes >= _BYTES_PER_GB:
        return f"{size_bytes / _BYTES_PER_GB:.2f} GB"
    if size_bytes >= _BYTES_PER_MB:
        return f"{size_bytes / _BYTES_PER_MB:.2f} MB"
    if size_bytes >= _BYTES_PER_KB:
        return f"{size_bytes / _BYTES_PER_KB:.1f} KB"
    return f"{size_bytes} B"


def _filename(key: str) -> str:
    """Return the base filename portion of an S3 object key.

    Args:
        key: Full S3 object key (may contain path separators).

    Returns:
        The last path segment of the key, or the key itself if no separator.
    """
    return key.split("/")[-1] or key


def _render_connection_form() -> Optional[S3Client]:
    """Render the S3 connection form in the sidebar.

    Lets the user enter endpoint, credentials, bucket, and prefix.
    On successful connection, stores the S3Client in session_state and
    returns it. Returns None if no client has been created yet or if
    the connection attempt fails.

    Returns:
        An authenticated S3Client instance, or None.
    """
    st.sidebar.header("S3 Connection")

    with st.sidebar.form("s3_form"):
        endpoint_url = st.text_input(
            "Endpoint URL",
            placeholder="https://s3.example.com  (leave blank for AWS S3)",
            help="Leave blank to connect to standard AWS S3.",
        )
        access_key = st.text_input(
            "Access Key ID",
            placeholder="AKIAIOSFODNN7EXAMPLE",
        )
        secret_key = st.text_input(
            "Secret Access Key",
            type="password",
            placeholder="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        bucket = st.text_input("Bucket Name", placeholder="my-bucket")
        prefix = st.text_input(
            "Prefix / Folder",
            placeholder="folder/subfolder/  (optional)",
            help="Filter results to a specific folder prefix inside the bucket.",
        )
        connect = st.form_submit_button("Connect", use_container_width=True)

    if connect:
        if not access_key or not secret_key or not bucket:
            st.sidebar.error("Access Key, Secret Key and Bucket are required.")
            return None

        logger.info("User initiated connection to bucket='%s'.", bucket)
        with st.spinner("Connecting to S3…"):
            try:
                client = S3Client(
                    endpoint_url=endpoint_url.strip() or None,
                    access_key=access_key.strip(),
                    secret_key=secret_key.strip(),
                    bucket=bucket.strip(),
                )
                client.check_connection()
                st.session_state["s3_client"] = client
                st.session_state["s3_prefix"] = prefix.strip()
                st.sidebar.success("Connected successfully.")
                logger.info("Connection established to bucket='%s'.", bucket)
            except S3ConnectorError as exc:
                st.sidebar.error(f"Connection failed: {exc}")
                logger.error("Connection failed for bucket='%s': %s", bucket, exc)
                return None

    return st.session_state.get("s3_client")


def _build_tree(objects: List[S3Object], base_prefix: str) -> Dict:
    """Build a nested dict tree from a flat list of S3 objects.

    Each leaf node is an S3Object instance. Each intermediate node is a dict
    whose keys are child folder/file names.

    S3 folder-placeholder objects (keys ending with "/", typically zero-byte)
    are skipped so they do not shadow real folder dicts in the tree.

    If a folder segment was previously stored as an S3Object (edge case with
    some S3-compatible stores), it is replaced with a dict so that child
    objects can be nested correctly.

    Example output for keys ["a/b/file.txt", "a/other.csv"]:
        {"a": {"b": {"file.txt": <S3Object>}, "other.csv": <S3Object>}}

    Args:
        objects: Flat list of S3Object instances.
        base_prefix: The root prefix that was used to query S3. It is stripped
                     from each key before building the tree so that the tree
                     starts relative to that prefix.

    Returns:
        Nested dict representing the folder/file hierarchy.
    """
    tree: Dict = {}
    for obj in objects:
        # Skip S3 folder-placeholder objects (zero-byte keys ending with "/")
        if obj.key.endswith("/"):
            continue
        relative_key = (
            obj.key[len(base_prefix):]
            if obj.key.startswith(base_prefix)
            else obj.key
        )
        parts = [p for p in relative_key.split("/") if p]
        if not parts:
            continue
        node = tree
        for part in parts[:-1]:
            existing = node.get(part)
            if not isinstance(existing, dict):
                # Either missing or previously stored as a folder-placeholder
                # S3Object — replace with a real dict so children can be added.
                node[part] = {}
            node = node[part]
        node[parts[-1]] = obj
    return tree


def _render_file_row(obj: object, client: S3Client, depth: int = 0) -> None:
    """Render a single file row with name, size, date and download button.

    Accepts any object that exposes the S3Object attributes (key, size,
    last_modified) so that hot-reload class identity issues do not crash
    the render loop.

    Args:
        obj: An S3Object (or duck-typed equivalent) to display.
        client: An authenticated S3Client for downloading.
        depth: Nesting level; increases left indentation for hierarchy.
    """
    try:
        key: str = obj.key  # type: ignore[attr-defined]
        size: int = obj.size  # type: ignore[attr-defined]
        last_modified = obj.last_modified  # type: ignore[attr-defined]
    except AttributeError:
        logger.warning("Skipping unrecognised object in file browser: %r", obj)
        return

    margin_em = max(0, depth) * 1.25
    cols = st.columns([5, 2, 3, 2])
    cols[0].markdown(
        f'<div style="margin-left:{margin_em}em">📄 `{_filename(key)}`</div>',
        unsafe_allow_html=True,
    )
    cols[1].text(_format_size(size))
    cols[2].text(last_modified.strftime("%Y-%m-%d %H:%M"))
    if cols[3].button("⬇ Download", key=f"dl_{key}", use_container_width=True):
        _download_object(client, key)


def _render_tree(tree: Dict, client: S3Client, depth: int = 0, path: str = "") -> None:
    """Recursively render a folder tree.

    Top-level folders use ``st.expander`` so the first hierarchy level opens
    in collapsible panels. Nested folders use toggle buttons persisted in
    ``st.session_state`` (nested expanders are avoided). Rows are indented by
    depth; files show a document icon and folders a folder icon.

    Args:
        tree: Nested dict produced by _build_tree.
        client: An authenticated S3Client for downloading.
        depth: Current recursion depth, used for indentation and unique keys.
        path: Slash-joined path of ancestor folder names, used to build unique
              session_state keys for each folder toggle.
    """
    folders = [(k, v) for k, v in sorted(tree.items()) if isinstance(v, dict)]
    # Treat any non-dict leaf as a file regardless of its exact type, so that
    # dataclass identity issues after hot-reloads do not drop items silently.
    files = [(k, v) for k, v in sorted(tree.items()) if not isinstance(v, dict)]

    if depth == 0:
        for folder_name, subtree in folders:
            folder_path = f"{path}/{folder_name}"
            file_count = _count_files(subtree)
            exp_label = (
                f"📁  {folder_name}  —  {file_count} "
                f"file{'s' if file_count != 1 else ''}"
            )
            with st.expander(exp_label, expanded=False):
                _render_tree(subtree, client, depth + 1, folder_path)

        if files:
            margin_em = max(0, depth) * 1.25
            header = st.columns([5, 2, 3, 2])
            header[0].markdown(
                f'<div style="margin-left:{margin_em}em"><strong>Name</strong></div>',
                unsafe_allow_html=True,
            )
            header[1].markdown("<strong>Size</strong>", unsafe_allow_html=True)
            header[2].markdown("<strong>Last modified</strong>", unsafe_allow_html=True)
            header[3].markdown("<strong>Download</strong>", unsafe_allow_html=True)
            for _, obj in files:
                _render_file_row(obj, client, depth=depth)
        return

    for folder_name, subtree in folders:
        folder_path = f"{path}/{folder_name}"
        state_key = f"folder_open_{folder_path}"
        is_open = st.session_state.get(state_key, False)
        file_count = _count_files(subtree)
        icon = "📂" if is_open else "📁"
        indent = "\u00a0" * (depth * 4)
        label = (
            f"{indent}{icon}  {folder_name}  —  {file_count} "
            f"file{'s' if file_count != 1 else ''}"
        )

        if st.button(
            label,
            key=f"folder_btn_{folder_path}",
            use_container_width=True,
        ):
            st.session_state[state_key] = not is_open

        if st.session_state.get(state_key, False):
            _render_tree(subtree, client, depth + 1, folder_path)

    if files:
        margin_em = max(0, depth) * 1.25
        header = st.columns([5, 2, 3, 2])
        header[0].markdown(
            f'<div style="margin-left:{margin_em}em"><strong>Name</strong></div>',
            unsafe_allow_html=True,
        )
        header[1].markdown("<strong>Size</strong>", unsafe_allow_html=True)
        header[2].markdown("<strong>Last modified</strong>", unsafe_allow_html=True)
        header[3].markdown("<strong>Download</strong>", unsafe_allow_html=True)
        for _, obj in files:
            _render_file_row(obj, client, depth=depth)


def _count_files(tree: object) -> int:
    """Count the total number of leaf files in a tree dict recursively.

    Accepts any value: returns 0 immediately for non-dict inputs so that
    defensive callers never trigger an AttributeError.

    Args:
        tree: Nested dict produced by _build_tree, or any leaf value.

    Returns:
        Total number of S3Object leaves under this node.
    """
    if not isinstance(tree, dict):
        return 0
    total = 0
    for v in tree.values():
        if isinstance(v, dict):
            total += _count_files(v)
        else:
            # Treat any non-dict leaf as a file (S3Object or unexpected type)
            total += 1
    return total


def _render_file_browser(client: S3Client) -> None:
    """Render the folder tree browser with expandable directories and download buttons.

    Fetches all objects from S3, optionally filters them by the search input,
    builds a folder hierarchy with root-level expanders, indented subtrees,
    and download buttons.

    Args:
        client: An authenticated S3Client instance.
    """
    prefix: str = st.session_state.get("s3_prefix", "")

    st.subheader("Files" + (f" — `{prefix}`" if prefix else ""))

    search = st.text_input(
        "🔍 Filter by name",
        placeholder="Type to filter…",
        label_visibility="collapsed",
    )

    with st.spinner("Loading file list…"):
        try:
            objects: List[S3Object] = client.list_objects(prefix=prefix)
        except S3ConnectorError as exc:
            st.error(f"Could not list files: {exc}")
            logger.error("Failed to list objects: %s", exc)
            return

    if not objects:
        st.info("No files found for this prefix.")
        return

    if search:
        query = search.lower()
        objects = [o for o in objects if query in o.key.lower()]

    if not objects:
        st.warning("No files match your filter.")
        return

    st.caption(f"{len(objects)} file(s) found.")
    st.divider()

    tree = _build_tree(objects, base_prefix=prefix)
    _render_tree(tree, client, depth=0, path="")


def _download_object(client: S3Client, key: str) -> None:
    """Fetch an object from S3 and trigger a browser download.

    Retrieves the raw bytes for the given key and uses st.download_button to
    push the file to the user's browser.

    Args:
        client: An authenticated S3Client instance.
        key: The full S3 object key to download.
    """
    with st.spinner(f"Downloading `{key}`…"):
        try:
            data = client.get_object_bytes(key)
        except S3ConnectorError as exc:
            st.error(f"Download failed: {exc}")
            logger.error("Download failed for key='%s': %s", key, exc)
            return

    st.download_button(
        label=f"Save {_filename(key)}",
        data=data,
        file_name=_filename(key),
        key=f"save_{key}",
    )
    logger.info("Object key='%s' ready for browser download.", key)


def main() -> None:
    """Run the S3 Connector Streamlit application.

    Initialises the page, enforces authentication, renders the connection
    form, and displays the file browser once connected.
    """
    st.set_page_config(
        page_title="S3 Connector",
        page_icon="🗂️",
        layout="wide",
    )

    if not render_login_page():
        st.stop()

    st.title("S3 Connector")

    client = _render_connection_form()

    if client is None:
        st.info("Configure your S3 connection in the sidebar to get started.")
        return

    _render_file_browser(client)

    st.sidebar.divider()
    if st.sidebar.button("Disconnect", use_container_width=True):
        for key in ("s3_client", "s3_prefix"):
            st.session_state.pop(key, None)
        logger.info("User disconnected from S3.")
        st.rerun()


if __name__ == "__main__":
    main()
