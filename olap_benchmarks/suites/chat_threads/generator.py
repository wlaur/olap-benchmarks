import json
from base64 import b64encode
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from random import Random
from uuid import UUID

import numpy as np

from .corpus import (
    ADJECTIVES,
    ASSISTANT_TEMPLATES,
    CODE_TEMPLATES,
    COMMON_WORDS,
    CONTENT_SCHEMA,
    MEDIA_TYPES,
    MODELS,
    NOUNS,
    STOP_REASONS,
    TAGS,
    THINKING_TEMPLATES,
    TITLE_TEMPLATES,
    TOPICS,
    USER_TEMPLATES,
    VEGA_MARKS,
    VEGA_SCHEMA_URL,
    VERBS,
    Topic,
)

BASE_USER_COUNT = 5_000
BASE_THREAD_COUNT = 20_000

ANCHOR_USER_COUNT = 10
ANCHOR_THREADS_PER_USER = 40
SACRIFICIAL_THREAD_COUNT = 1_000

WINDOW_END = datetime(2026, 1, 1)
WINDOW_DAYS = 730

MEAN_THREAD_MESSAGES = 12.5
MIN_THREAD_MESSAGES = 2
MAX_THREAD_MESSAGES = 200
ANCHOR_MIN_MESSAGES = 30
ANCHOR_MAX_MESSAGES = 80

VISUALIZATION_SHARE = 0.06
ATTACHMENT_SHARE = 0.012
THINKING_SHARE = 0.35
TOOL_TURN_SHARE = 0.18
BRANCH_SHARE = 0.05

UUID_NAMESPACE = UUID("6f4c9d1e-0b2a-4c6f-9a1d-3e5b7c8f2a04")

DIURNAL_WEIGHTS: tuple[float, ...] = (
    0.2,
    0.1,
    0.1,
    0.1,
    0.2,
    0.4,
    0.9,
    1.8,
    3.2,
    4.1,
    4.4,
    4.0,
    3.6,
    3.9,
    4.2,
    4.0,
    3.4,
    2.6,
    2.0,
    1.7,
    1.5,
    1.2,
    0.8,
    0.4,
)
WEEKDAY_WEIGHTS: tuple[float, ...] = (1.0, 1.05, 1.05, 1.0, 0.85, 0.35, 0.3)


@dataclass(frozen=True)
class ChatGenerationSpec:
    scale_factor: int
    seed: int
    user_count: int
    thread_count: int
    thread_counts_per_user: tuple[int, ...]
    thread_offsets: tuple[int, ...]

    @property
    def sacrificial_start_index(self) -> int:
        return self.thread_count - SACRIFICIAL_THREAD_COUNT


@dataclass(frozen=True)
class ThreadRow:
    thread_id: str
    user_id: str
    created_at: datetime
    updated_at: datetime
    title: str
    message_count: int
    settings: str


@dataclass(frozen=True)
class MessageRow:
    thread_id: str
    message_id: str
    user_id: str
    seq: int
    parent_id: str | None
    created_at: datetime
    content: str


def deterministic_uuid(kind: str, index: int) -> str:
    from uuid import uuid5

    return str(uuid5(UUID_NAMESPACE, f"{kind}:{index}"))


def message_uuid(thread_index: int, seq: int) -> str:
    return deterministic_uuid("message", thread_index * (MAX_THREAD_MESSAGES + 1) + seq)


def _thread_count_distribution(user_count: int, thread_count: int, seed: int) -> tuple[int, ...]:
    anchor_total = ANCHOR_USER_COUNT * ANCHOR_THREADS_PER_USER
    remaining_users = user_count - ANCHOR_USER_COUNT
    remaining_threads = thread_count - anchor_total

    rng = np.random.default_rng(np.random.SeedSequence([seed, 1]))
    weights = rng.lognormal(mean=0.0, sigma=0.9, size=remaining_users)
    scaled = np.maximum(1, np.round(weights * (remaining_threads / weights.sum()))).astype(np.int64)

    # round-off drift is corrected on the least active users so the anchor profile stays exact
    drift = int(scaled.sum()) - remaining_threads
    order = np.argsort(scaled, kind="stable")
    position = 0
    while drift != 0:
        idx = int(order[position % remaining_users])
        if drift > 0 and scaled[idx] > 1:
            scaled[idx] -= 1
            drift -= 1
        elif drift < 0:
            scaled[idx] += 1
            drift += 1
        position += 1

    return (*([ANCHOR_THREADS_PER_USER] * ANCHOR_USER_COUNT), *(int(value) for value in scaled))


def build_chat_generation_spec(scale_factor: int, seed: int = 1) -> ChatGenerationSpec:
    user_count = BASE_USER_COUNT * scale_factor
    thread_count = BASE_THREAD_COUNT * scale_factor
    thread_counts = _thread_count_distribution(user_count, thread_count, seed)

    offsets: list[int] = []
    running = 0
    for count in thread_counts:
        offsets.append(running)
        running += count

    return ChatGenerationSpec(
        scale_factor=scale_factor,
        seed=seed,
        user_count=user_count,
        thread_count=thread_count,
        thread_counts_per_user=thread_counts,
        thread_offsets=tuple(offsets),
    )


class TextGenerator:
    def __init__(self, rng: Random, topic: Topic) -> None:
        self._rng = rng
        self._topic = topic

    def _slots(self) -> dict[str, str]:
        rng = self._rng
        term, term2 = rng.sample(self._topic.terms, 2)
        return {
            "term": term,
            "Term": term[0].upper() + term[1:],
            "term2": term2,
            "verb": rng.choice(VERBS),
            "adj": rng.choice(ADJECTIVES),
            "adj2": rng.choice(ADJECTIVES),
            "noun": rng.choice(NOUNS),
            "num": str(rng.randrange(2, 500_000)),
            "metric": rng.choice(self._topic.metrics),
            "word": rng.choice(COMMON_WORDS),
        }

    def _sentence(self, templates: tuple[str, ...]) -> str:
        return templates[self._rng.randrange(len(templates))].format(**self._slots())

    def _filler(self, word_count: int) -> str:
        rng = self._rng
        words = [rng.choice(COMMON_WORDS) for _ in range(word_count)]
        words[0] = words[0][0].upper() + words[0][1:]
        return " ".join(words) + "."

    def paragraph(self, templates: tuple[str, ...], sentence_count: int) -> str:
        rng = self._rng
        parts: list[str] = []
        for _ in range(sentence_count):
            parts.append(self._sentence(templates))
            if rng.random() < 0.35:
                parts.append(self._filler(rng.randrange(6, 22)))
        return " ".join(parts)

    def bullet_list(self, item_count: int) -> str:
        return "\n".join(f"- {self._sentence(ASSISTANT_TEMPLATES)}" for _ in range(item_count))

    def code_block(self) -> str:
        rng = self._rng
        language, template = CODE_TEMPLATES[rng.randrange(len(CODE_TEMPLATES))]
        body = template.format(
            ident=rng.choice(NOUNS).replace(" ", "_"),
            word=rng.choice(COMMON_WORDS),
            metric=rng.choice(self._topic.metrics),
            num=rng.randrange(8, 100_000),
            limit=rng.randrange(5, 500),
            date=f"2025-{rng.randrange(1, 13):02d}-{rng.randrange(1, 29):02d}",
        )
        return f"```{language}\n{body}\n```"

    def title(self) -> str:
        return self._sentence(TITLE_TEMPLATES)[:120]

    def user_text(self) -> str:
        rng = self._rng
        blocks = [self.paragraph(USER_TEMPLATES, rng.randrange(1, 4))]
        if rng.random() < 0.18:
            blocks.append(self.code_block())
        return "\n\n".join(blocks)

    def assistant_text(self) -> str:
        rng = self._rng
        blocks = [self.paragraph(ASSISTANT_TEMPLATES, rng.randrange(2, 7))]
        if rng.random() < 0.30:
            blocks.append(self.bullet_list(rng.randrange(2, 6)))
        if rng.random() < 0.28:
            blocks.append(self.code_block())
        if rng.random() < 0.45:
            blocks.append(self.paragraph(ASSISTANT_TEMPLATES, rng.randrange(1, 4)))
        return "\n\n".join(blocks)


def _visualization_part(rng: Random, topic: Topic, text: TextGenerator) -> dict[str, object]:
    record_count = rng.randrange(50, 500)
    metric = rng.choice(topic.metrics)
    category_pool = [term.replace(" ", "-") for term in rng.sample(topic.terms, 8)]
    values = [
        {
            "category": rng.choice(category_pool),
            "period": f"2025-{rng.randrange(1, 13):02d}",
            metric: round(rng.uniform(0.0, 10_000.0), 3),
            "count": rng.randrange(1, 5_000),
        }
        for _ in range(record_count)
    ]
    return {
        "type": "visualization",
        "mime": "application/vnd.vegalite.v5+json",
        "caption": text.title(),
        "spec": {
            "$schema": VEGA_SCHEMA_URL,
            "description": text.title(),
            "mark": rng.choice(VEGA_MARKS),
            "encoding": {
                "x": {"field": "period", "type": "ordinal"},
                "y": {"field": metric, "type": "quantitative"},
                "color": {"field": "category", "type": "nominal"},
            },
            "data": {"values": values},
        },
    }


def _attachment_source(rng: Random) -> dict[str, object]:
    roll = rng.random()
    cumulative = 0.0
    media_type, extension = MEDIA_TYPES[0][0], MEDIA_TYPES[0][1]
    for candidate_type, candidate_extension, share in MEDIA_TYPES:
        cumulative += share
        if roll <= cumulative:
            media_type, extension = candidate_type, candidate_extension
            break

    if media_type == "text/csv":
        row_count = rng.randrange(200, 4_000)
        header = "id,category,period,value,count\n"
        rows = "".join(
            f"{idx},{rng.choice(COMMON_WORDS)},2025-{rng.randrange(1, 13):02d},"
            f"{round(rng.uniform(0, 1000), 2)},{rng.randrange(1, 999)}\n"
            for idx in range(row_count)
        )
        payload = (header + rows).encode()
    else:
        # png and pdf payloads are already compressed in the real world, so random bytes are
        # the honest stand-in: base64 of them is incompressible, exactly like the real thing
        payload = rng.randbytes(rng.randrange(30_000, 200_000))

    return {
        "type": "base64",
        "media_type": media_type,
        "file_name": f"{rng.choice(COMMON_WORDS)}-{rng.randrange(1000, 9999)}.{extension}",
        "data": b64encode(payload).decode("ascii"),
    }


def _tool_use_part(rng: Random, topic: Topic, tool_call_id: str) -> dict[str, object]:
    return {
        "type": "tool_use",
        "id": tool_call_id,
        "name": rng.choice(topic.tools),
        "input": {
            "target": rng.choice(topic.terms),
            "limit": rng.randrange(10, 1_000),
            "filters": [rng.choice(COMMON_WORDS) for _ in range(rng.randrange(1, 4))],
        },
    }


def _user_message_content(rng: Random, text: TextGenerator, force_attachment: bool) -> str:
    parts: list[dict[str, object]] = [{"type": "text", "text": text.user_text()}]
    if force_attachment or rng.random() < ATTACHMENT_SHARE:
        parts.append({"type": "image", "source": _attachment_source(rng)})

    return json.dumps(
        {"schema": CONTENT_SCHEMA, "role": "user", "parts": parts},
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _assistant_message_content(
    rng: Random,
    topic: Topic,
    text: TextGenerator,
    *,
    tool_call_id: str | None,
    force_visualization: bool,
) -> str:
    parts: list[dict[str, object]] = []
    if rng.random() < THINKING_SHARE:
        parts.append({"type": "thinking", "text": text.paragraph(THINKING_TEMPLATES, rng.randrange(1, 4))})

    parts.append({"type": "text", "text": text.assistant_text()})

    if force_visualization or rng.random() < VISUALIZATION_SHARE:
        parts.append(_visualization_part(rng, topic, text))

    if tool_call_id is not None:
        parts.append(_tool_use_part(rng, topic, tool_call_id))

    input_tokens = rng.randrange(400, 60_000)
    output_tokens = rng.randrange(40, 4_000)

    return json.dumps(
        {
            "schema": CONTENT_SCHEMA,
            "role": "assistant",
            "model": rng.choice(MODELS),
            "stop_reason": "tool_use" if tool_call_id is not None else rng.choice(STOP_REASONS),
            "usage": {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cache_read_tokens": rng.randrange(0, input_tokens),
            },
            "latency_ms": rng.randrange(300, 90_000),
            "parts": parts,
        },
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _tool_message_content(rng: Random, text: TextGenerator, tool_call_id: str, force_attachment: bool) -> str:
    is_error = rng.random() < 0.09
    result_parts: list[dict[str, object]] = [
        {"type": "text", "text": text.paragraph(ASSISTANT_TEMPLATES, rng.randrange(1, 3))}
    ]
    if force_attachment or rng.random() < ATTACHMENT_SHARE * 3:
        result_parts.append({"type": "image", "source": _attachment_source(rng)})

    return json.dumps(
        {
            "schema": CONTENT_SCHEMA,
            "role": "tool",
            "parts": [
                {
                    "type": "tool_result",
                    "tool_use_id": tool_call_id,
                    "is_error": is_error,
                    "content": result_parts,
                }
            ],
        },
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _thread_settings(rng: Random, topic: Topic, text: TextGenerator) -> str:
    return json.dumps(
        {
            "tags": rng.sample(TAGS, rng.randrange(0, 4)),
            "pinned": rng.random() < 0.05,
            "archived": rng.random() < 0.12,
            "topic": topic.name,
            "model_preference": rng.choice(MODELS),
            "system_prompt": text.paragraph(ASSISTANT_TEMPLATES, 1) if rng.random() < 0.30 else None,
        },
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _thread_started_at(rng: Random) -> datetime:
    day_offset = rng.randrange(WINDOW_DAYS)
    day = WINDOW_END - timedelta(days=WINDOW_DAYS - day_offset)
    if rng.random() > WEEKDAY_WEIGHTS[day.weekday()]:
        day = day - timedelta(days=rng.randrange(1, 4))

    hour = rng.choices(range(24), weights=DIURNAL_WEIGHTS, k=1)[0]
    return day.replace(hour=hour) + timedelta(
        minutes=rng.randrange(60), seconds=rng.randrange(60), milliseconds=rng.randrange(1000)
    )


def _message_gap(rng: Random) -> timedelta:
    if rng.random() < 0.08:
        return timedelta(seconds=rng.randrange(3_600, 3 * 86_400))
    return timedelta(seconds=rng.randrange(3, 240), milliseconds=rng.randrange(1000))


def _thread_message_count(rng: Random, is_anchor: bool) -> int:
    if is_anchor:
        return rng.randrange(ANCHOR_MIN_MESSAGES, ANCHOR_MAX_MESSAGES + 1)

    drawn = int(round(rng.lognormvariate(np.log(MEAN_THREAD_MESSAGES) - 0.5 * 0.8**2, 0.8)))
    return max(MIN_THREAD_MESSAGES, min(MAX_THREAD_MESSAGES, drawn))


def generate_thread(
    spec: ChatGenerationSpec,
    user_index: int,
    thread_index: int,
    local_thread_index: int,
) -> tuple[ThreadRow, list[MessageRow]]:
    rng = Random(f"{spec.seed}:{thread_index}")
    topic = TOPICS[(thread_index + user_index) % len(TOPICS)]
    text = TextGenerator(rng, topic)

    is_anchor = user_index < ANCHOR_USER_COUNT
    user_id = deterministic_uuid("user", user_index)
    thread_id = deterministic_uuid("thread", thread_index)
    message_count = _thread_message_count(rng, is_anchor)

    # anchor threads always carry the rich content the point-lookup queries are meant to read
    forced_visualization_seq = 2 if is_anchor else -1
    forced_attachment_seq = 3 if is_anchor else -1

    created_at = _thread_started_at(rng)
    current_at = created_at

    messages: list[MessageRow] = []
    pending_tool_call: str | None = None
    previous_id: str | None = None
    branch_candidates: list[str] = []

    for seq in range(1, message_count + 1):
        message_id = message_uuid(thread_index, seq)

        if pending_tool_call is not None:
            content = _tool_message_content(rng, text, pending_tool_call, force_attachment=seq == forced_attachment_seq)
            pending_tool_call = None
        elif seq % 2 == 1:
            content = _user_message_content(rng, text, force_attachment=seq == forced_attachment_seq)
        else:
            wants_tool = rng.random() < TOOL_TURN_SHARE or (is_anchor and seq == 4)
            tool_call_id = f"toolu_{deterministic_uuid('tool', thread_index * 1000 + seq)[:16]}" if wants_tool else None
            content = _assistant_message_content(
                rng,
                topic,
                text,
                tool_call_id=tool_call_id,
                force_visualization=seq == forced_visualization_seq,
            )
            pending_tool_call = tool_call_id

        parent_id = previous_id
        if branch_candidates and rng.random() < BRANCH_SHARE:
            parent_id = rng.choice(branch_candidates)

        messages.append(
            MessageRow(
                thread_id=thread_id,
                message_id=message_id,
                user_id=user_id,
                seq=seq,
                parent_id=parent_id,
                created_at=current_at,
                content=content,
            )
        )

        if previous_id is not None:
            branch_candidates.append(previous_id)
        previous_id = message_id
        current_at = current_at + _message_gap(rng)

    _ = local_thread_index

    thread = ThreadRow(
        thread_id=thread_id,
        user_id=user_id,
        created_at=created_at,
        updated_at=messages[-1].created_at,
        title=text.title(),
        message_count=len(messages),
        settings=_thread_settings(rng, topic, text),
    )

    return thread, messages


def iter_generated_rows(spec: ChatGenerationSpec) -> Iterator[tuple[ThreadRow, list[MessageRow]]]:
    for user_index in range(spec.user_count):
        offset = spec.thread_offsets[user_index]
        for local_thread_index in range(spec.thread_counts_per_user[user_index]):
            yield generate_thread(spec, user_index, offset + local_thread_index, local_thread_index)
