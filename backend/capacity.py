from datetime import date, datetime

# ─── Tier lookup table ────────────────────────────────────────────────────────

AGE_TIER_TABLE = [
    (18, 25, 3),
    (26, 35, 5),
    (36, 45, 6),
    (46, 60, 4),
]

MAX_TIER = 7
DIFFICULTY_PER_TIER = 18


def compute_age(dob: date) -> int:
    today = date.today()
    age = today.year - dob.year
    if (today.month, today.day) < (dob.month, dob.day):
        age -= 1
    return age


def parse_dob(dob_str: str) -> date:
    """Accept DD/MM/YYYY only. Raises ValueError if format is wrong."""
    try:
        return datetime.strptime(dob_str.strip(), "%d/%m/%Y").date()
    except ValueError:
        raise ValueError(f"Invalid DOB format: '{dob_str}'. Use DD/MM/YYYY.")


def get_capacity(dob_str: str, experience_years: int) -> dict:
    """
    Compute driver capacity from DOB and experience.

    Returns:
        age, capacity_tier, max_single_route_difficulty, experience_bonus_applied

    Raises:
        ValueError if age is outside 18–60.
    """
    dob = parse_dob(dob_str)
    age = compute_age(dob)

    if age < 18:
        raise ValueError(f"Driver is {age} years old — minimum age is 18.")
    if age > 60:
        raise ValueError(f"Driver is {age} years old — maximum onboarding age is 60.")

    base_tier = None
    for (low, high, tier) in AGE_TIER_TABLE:
        if low <= age <= high:
            base_tier = tier
            break

    if base_tier is None:
        raise ValueError(f"No tier defined for age {age}.")

    experience_bonus = experience_years >= 5
    capacity_tier = min(base_tier + (1 if experience_bonus else 0), MAX_TIER)
    max_difficulty = capacity_tier * DIFFICULTY_PER_TIER

    return {
        "age":                         age,
        "capacity_tier":               capacity_tier,
        "max_single_route_difficulty": max_difficulty,
        "experience_bonus_applied":    experience_bonus,
    }