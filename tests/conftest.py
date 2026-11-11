import os

from hypothesis import settings

# "ci": fixed seed, so a red build is reproducible and a green one is not luck-of-the-draw.
# "nightly": random seed and more examples, to keep exploring (see .github/workflows).
settings.register_profile("ci", derandomize=True, print_blob=True)
settings.register_profile("nightly", derandomize=False, print_blob=True)
settings.register_profile("dev", print_blob=True)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "dev"))
