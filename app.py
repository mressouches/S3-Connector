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
from typing import List, Optional

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


def _render_file_browser(client: S3Client) -> None:
    """Render the file browser table and download buttons.

    Fetches the object list from S3, applies the user's name filter, and
    displays each object with its size, modification date, and a download
    button.

    Args:
        client: An authenticated S3Client instance.
    """
    prefix: str = st.session_state.get("s3_prefix", "")

    st.subheader("Files" + (f" — `{prefix}`" if prefix else ""))

    search = st.text_input(
        "Filter by name",
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

    header_cols = st.columns([5, 2, 3, 2])
    header_cols[0].markdown("**Name**")
    header_cols[1].markdown("**Size**")
    header_cols[2].markdown("**Last modified**")
    header_cols[3].markdown("**Download**")

    st.divider()

    for obj in objects:
        cols = st.columns([5, 2, 3, 2])
        cols[0].markdown(f"`{obj.key}`")
        cols[1].text(_format_size(obj.size))
        cols[2].text(obj.last_modified.strftime("%Y-%m-%d %H:%M"))

        dl_key = f"dl_{obj.key}"
        if cols[3].button("⬇ Download", key=dl_key, use_container_width=True):
            _download_object(client, obj.key)


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
