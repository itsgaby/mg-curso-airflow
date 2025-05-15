from datetime import datetime
import pytz

# if execution_date.day_of_week > 4:  # 5 = sábado, 6 = domingo

# M T W T F S S
# 0 1 2 3 4 5 6

# Input string
iso_string = "2025-05-02T00:00:00+00:00"

# Parse the ISO string
execution_date = datetime.fromisoformat(iso_string)

# Get day of the week (Monday=0, Saturday=5 ,Sunday=6)
day_of_week = execution_date.weekday()

print(iso_string, day_of_week)

# ----------------------------
# M T W T F S S
# 1 2 3 4 5 6 7

# Input string
iso_string = "2025-05-02T00:00:00+00:00"

# Parse the ISO string
execution_date = datetime.fromisoformat(iso_string)

# Get day of the week (Monday=1, Sunday=7)
day_of_week = execution_date.isoweekday()

print(iso_string, day_of_week)

