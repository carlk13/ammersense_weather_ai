This project started as a way to combine my interest in meteorological data with edge computing. The goal was to build a system that doesn't just display weather data, but actually "understands" the microclimate of the Ammersee region here in Bavaria.

Instead of relying on generic weather apps, I built a pipeline that scrapes hyper-local data from the Zebrafell station in Herrsching (shoutout to that repository!) and runs local AI inference to see how the lake's specific conditions affect wind and temperature trends.
🛠 What’s inside?

I designed this as a modular microservices architecture using Docker. This way, if the Ingest service crashes, the Database and Dashboard stay up.

    The Ingestor: A Python script that polls the Zebrafell API every 10 minutes. It handles the "cold start" issues of Render-hosted APIs and converts UTC data to our local Bavarian time.

    The Brain (AI): Using a ClimaX Adapter. ClimaX is a foundation model, and I've adapted it to run on the Pi to predict short-term changes based on historical patterns since 2007.

    The Storage: InfluxDB v2. I chose a time-series DB because traditional SQL gets bogged down when you have 19 years of 10-minute weather intervals.

    The UI: A Streamlit dashboard. It’s simple, written in pure Python, and perfect for viewing on a phone while at the lake.

🏗 System Architecture

I used Mermaid to map out how the containers talk to each other over the virtual weather_net.
```mermaid
graph TD
    %% Define Node Styles
    classDef database fill:#e1f5fe,stroke:#01579b,stroke-width:2px,color:#01579b;
    classDef service fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#2e7d32;
    classDef ai fill:#fff3e0,stroke:#ef6c00,stroke-width:2px,color:#ef6c00;
    classDef dashboard fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,color:#7b1fa2;
    classDef volume fill:#eceff1,stroke:#455a64,stroke-width:2px,stroke-dasharray: 5 5,color:#455a64;
    classDef external fill:#ffffff,stroke:#333,stroke-width:2px;

    subgraph RPi ["Raspberry Pi (Docker Host)"]
        direction TB
        
        subgraph Net [" Internal Network: weather_net"]
            direction LR
            DB[(InDB v2)]:::database
            Ingest["Ingest Service<br/>(Open-Meteo)"]:::service
            AI["AI Engine<br/>(ClimaX Adapter)"]:::ai
            Dash["Dashboard<br/>(Streamlit)"]:::dashboard
        end
        
        Vol1[/"vol: influx_data"\]:::volume
        Vol2[/"vol: model_weights"\]:::volume
    end

    %% External Connections
    Internet((Internet)):::external
    User((User)):::external

    %% Relationships
    Internet -->|JSON API| Ingest
    Ingest -->|Write| DB
    DB <-->|Query| AI
    AI -->|Predict| Dash
    Dash <-->|Read| DB
    
    %% Volume Links
    Vol1 -.->|Persist| DB
    Vol2 -.->|Mount| AI

    %% User Interaction
    User -->|Browser :8501| Dash

    %% Legend/Note Styling
    style RPi fill:#f9f9f9,stroke:#666,stroke-width:3px
    style Net fill:#fff,stroke:#ddd,stroke-dasharray: 5 5
```