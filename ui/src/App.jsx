import { useState } from "react";
import axios from "axios";
import "./App.css";

const API = "http://localhost:8000";

const EVENT_COLORS = {
  encounter: "#4f86c6",
  lab_visit: "#5aab61",
  note: "#e07b39",
};

// Confidence badge — used for NLP-extracted entities
function ConfidenceBadge({ isInferred }) {
  if (isInferred === null || isInferred === undefined) return null;
  return (
    <span className={`confidence-badge ${isInferred ? "inferred" : "stated"}`}>
      {isInferred ? "inferred" : "stated"}
    </span>
  );
}

// Tooltip that shows source_text on hover
function SourceTooltip({ text, sourceText }) {
  if (!sourceText) return <span>{text}</span>;
  return (
    <span className="has-tooltip">
      {text}
      <span className="tooltip">{sourceText}</span>
    </span>
  );
}

function PatientCard({ patient }) {
  return (
    <div className="patient-card">
      <div className="patient-name">{patient.patient_name || "Unknown"}</div>
      <div className="patient-meta">
        <span>{patient.age ? `${patient.age} yrs` : "Age unknown"}</span>
        <span>{patient.gender || "Gender unknown"}</span>
        {patient.dob && <span>DOB: {patient.dob}</span>}
        <span className="folder-tag">{patient.folder_id}</span>
      </div>
      <div className="counts">
        <div className="count-badge">
          <span className="count-value">{patient.encounter_count}</span>
          <span className="count-label">Encounters</span>
        </div>
        <div className="count-badge">
          <span className="count-value">{patient.lab_result_count}</span>
          <span className="count-label">Labs</span>
        </div>
        <div className="count-badge abnormal">
          <span className="count-value">{patient.abnormal_lab_count}</span>
          <span className="count-label">Abnormal</span>
        </div>
        <div className="count-badge">
          <span className="count-value">{patient.note_count}</span>
          <span className="count-label">Notes</span>
        </div>
      </div>
    </div>
  );
}

function TimelineEvent({ event }) {
  const color = EVENT_COLORS[event.event_type] || "#888";
  return (
    <div className="timeline-event">
      <div className="event-dot" style={{ backgroundColor: color }} />
      <div className="event-body">
        <div className="event-header">
          <span className="event-date">{event.event_date || "No date"}</span>
          <span className="event-type-chip" style={{ backgroundColor: color }}>
            {event.event_type}
          </span>
          {event.event_subtype && (
            <span className="event-subtype">{event.event_subtype}</span>
          )}
        </div>
        {event.description && (
          <div className="event-description">{event.description}</div>
        )}
        {event.provider && (
          <div className="event-meta">Provider: {event.provider}</div>
        )}
        {event.outcome && (
          <div className="event-meta">Outcome: {event.outcome}</div>
        )}
      </div>
    </div>
  );
}

// NLP entity row with confidence badge + source tooltip
function EntityRow({ entity }) {
  return (
    <div className="entity-row">
      <SourceTooltip text={entity.entity_value} sourceText={entity.source_text} />
      <ConfidenceBadge isInferred={entity.is_inferred} />
    </div>
  );
}

function FolderTimeline({ folder }) {
  return (
    <div className="folder-timeline">
      <div className="timeline-line">
        {folder.events.map((event, i) => (
          <TimelineEvent key={i} event={event} />
        ))}
      </div>
    </div>
  );
}

export default function App() {
  const [query, setQuery] = useState("");
  const [patients, setPatients] = useState(null);
  const [timeline, setTimeline] = useState(null);
  const [activeFolder, setActiveFolder] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  async function search(e) {
    e.preventDefault();
    if (!query.trim()) return;

    setLoading(true);
    setError(null);
    setPatients(null);
    setTimeline(null);
    setActiveFolder(null);

    try {
      const [patientRes, timelineRes] = await Promise.all([
        axios.get(`${API}/patients/${query.trim()}`),
        axios.get(`${API}/patients/${query.trim()}/timeline`),
      ]);
      setPatients(patientRes.data);
      setTimeline(timelineRes.data);
      setActiveFolder(timelineRes.data.folders[0]?.folder_id ?? null);
    } catch (err) {
      if (err.response?.status === 404) {
        setError(`No patient found with ID "${query.trim()}"`);
      } else {
        setError("Something went wrong. Is the API running?");
      }
    } finally {
      setLoading(false);
    }
  }

  const activeTimeline = timeline?.folders.find(
    (f) => f.folder_id === activeFolder
  );
  const activePatient = patients?.results.find(
    (p) => p.folder_id === activeFolder
  );

  return (
    <div className="app">
      <header className="app-header">
        <h1>EHR Patient Timeline</h1>
      </header>

      <main className="app-main">
        <form className="search-form" onSubmit={search}>
          <input
            className="search-input"
            type="text"
            placeholder="Enter patient MRN..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
          <button className="search-btn" type="submit" disabled={loading}>
            {loading ? "Searching..." : "Search"}
          </button>
        </form>

        {error && <div className="error">{error}</div>}

        {patients && timeline && (
          <div className="results">
            {timeline.folders.length > 1 && (
              <div className="folder-tabs">
                {timeline.folders.map((f) => (
                  <button
                    key={f.folder_id}
                    className={`folder-tab ${f.folder_id === activeFolder ? "active" : ""}`}
                    onClick={() => setActiveFolder(f.folder_id)}
                  >
                    {f.folder_id}
                    <span className="tab-count">{f.event_count}</span>
                  </button>
                ))}
              </div>
            )}

            {activePatient && <PatientCard patient={activePatient} />}

            {activeTimeline && (
              <div className="timeline-section">
                <div className="timeline-heading">
                  Timeline — {activeTimeline.event_count} events
                </div>
                <FolderTimeline folder={activeTimeline} />
              </div>
            )}
          </div>
        )}
      </main>
    </div>
  );
}
