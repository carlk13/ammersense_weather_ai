FROM python:3.11-slim-bookworm

# 1. Install uv (The magic step)
# We copy the binary directly from the official uv image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 2. Set working directory
WORKDIR /app

# 3. Copy only dependency files first (for Docker caching)
# This means Docker won't re-install packages unless these specific files change
COPY pyproject.toml uv.lock ./

# 4. Install dependencies
# --frozen: fails if lockfile is out of sync (safety)
# --no-cache: saves space in the image
RUN uv sync --frozen --no-cache

# 5. Copy the rest of your application code
COPY . .

# 6. Set the path to use the virtual environment uv created
# By default, uv creates a venv at .venv
ENV PATH="/app/.venv/bin:$PATH"

# 7. Run your app
# (Change this CMD for each service: 'streamlit run app.py' for dashboard, etc.)
CMD ["python", "main.py"]