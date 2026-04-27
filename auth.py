"""
S3 Connector — App-level authentication
-----------------------------------------
Provides password verification for the Streamlit app.

The expected password is read from the APP_PASSWORD environment variable
(loaded from .env via python-dotenv). Comparison uses hmac.compare_digest
to prevent timing-based side-channel attacks.

Usage:
    from auth import is_password_correct, render_login_page
    if not render_login_page():
        st.stop()

Environment variables:
    APP_PASSWORD    Plain-text password that protects the app (required)
"""

import hmac
import logging
import os

import streamlit as st
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("s3connector.auth")


def is_password_correct(candidate: str) -> bool:
    """Check whether the supplied password matches the configured app password.

    Uses hmac.compare_digest to avoid timing attacks when comparing strings.

    Args:
        candidate: Password string entered by the user.

    Returns:
        True if the candidate matches APP_PASSWORD, False otherwise.

    Raises:
        RuntimeError: If APP_PASSWORD is not set in the environment.
    """
    expected = os.environ.get("APP_PASSWORD")
    if not expected:
        logger.critical("APP_PASSWORD environment variable is not set.")
        raise RuntimeError(
            "APP_PASSWORD is not configured. "
            "Copy .env.example to .env and set a password."
        )
    result = hmac.compare_digest(candidate.encode(), expected.encode())
    if result:
        logger.info("Successful login attempt.")
    else:
        logger.warning("Failed login attempt — incorrect password.")
    return result


def render_login_page() -> bool:
    """Render the login form and manage the authenticated session state.

    Displays a password input form when the user is not yet authenticated.
    On correct password entry, sets st.session_state["authenticated"] = True
    and returns True. Returns False when authentication has not yet succeeded
    so the caller can halt further rendering with st.stop().

    Returns:
        True if the current session is authenticated, False otherwise.
    """
    if st.session_state.get("authenticated"):
        return True

    st.title("S3 Connector")
    st.subheader("Login")

    with st.form("login_form"):
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Sign in")

    if submitted:
        try:
            if is_password_correct(password):
                st.session_state["authenticated"] = True
                logger.info("Session authenticated.")
                st.rerun()
            else:
                st.error("Incorrect password. Please try again.")
        except RuntimeError as exc:
            st.error(str(exc))
            logger.exception("Authentication setup error.")

    return False
