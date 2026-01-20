# settings.py
import os
from pathlib import Path
from dotenv import load_dotenv

# ======================
# Paths & .env
# ======================
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")  # layihə kökündə .env varsa yüklə

# ----------------------
# Env helper-lər
# ----------------------
def env(key: str, default=None, cast=None):
    val = os.getenv(key, default)
    if val is None:
        return None
    if cast:
        try:
            return cast(val)
        except Exception:
            return default
    return val

def env_bool(key: str, default: bool = False):
    return str(os.getenv(key, str(default))).strip().lower() in {"1", "true", "yes", "on"}

def env_list(key: str, default: str = ""):
    raw = os.getenv(key, default)
    return [x.strip() for x in raw.split(",") if x.strip()]

# ======================
# Core security (DEV)
# ======================
SECRET_KEY = env("SECRET_KEY", "dev-only-please-change")
DEBUG = env_bool("DEBUG", True)
ALLOWED_HOSTS = env_list("ALLOWED_HOSTS", "*")

# HTTPS cookie-ləri (DEV-də söndürülür)
SESSION_COOKIE_SECURE = not DEBUG
CSRF_COOKIE_SECURE = not DEBUG

# CSRF üçün etibar olunan origin-lər
CSRF_TRUSTED_ORIGINS = env_list("CSRF_TRUSTED_ORIGINS", "")

# Dev-də COOP/COEP xəbərdarlıqlarını susdur
if DEBUG:
    SECURE_CROSS_ORIGIN_OPENER_POLICY = None
    SECURE_CROSS_ORIGIN_EMBEDDER_POLICY = None

# ======================
# Apps / Middleware
# ======================
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'drf_spectacular',
    'corrections',
]

REST_FRAMEWORK = {
  "DEFAULT_SCHEMA_CLASS": "drf_spectacular.openapi.AutoSchema",
}

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'crrs.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / "templates"],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'crrs.wsgi.application'

# ======================
# Database (PostgreSQL)
# ======================
DATABASES = {
    'default': {
        'ENGINE':  env('DB_ENGINE',  'django.db.backends.postgresql'),
        'NAME':    env('DB_NAME',    None),
        'USER':    env('DB_USER',    None),
        'PASSWORD':env('DB_PASSWORD',None),
        'HOST':    env('DB_HOST',    'localhost'),
        'PORT':    env('DB_PORT',    '5432'),
    }
}

# ======================
# Internationalization
# ======================
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_TZ = True

# ======================
# Static files
# ======================
STATIC_URL = 'static/'
STATICFILES_DIRS = [BASE_DIR / 'static']
STATIC_ROOT = BASE_DIR / 'staticfiles'

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# ======================
# External service configs
# ======================
NODE_REDEEM_URL          = env("NODE_REDEEM_URL", "http://10.11.1.73:8080/api/requests/handoff/redeem")
NODE_REDEEM_METHOD       = env("NODE_REDEEM_METHOD", "FORM")           # FORM | JSON | GET
NODE_REDEEM_TIMEOUT      = env("NODE_REDEEM_TIMEOUT", "8", cast=int)
NODE_REDEEM_REQUIRE_TOKEN= env_bool("NODE_REDEEM_REQUIRE_TOKEN", True) # token mütləq olsun
NODE_REDEEM_EXP_SKEW_SEC = env("NODE_REDEEM_EXP_SKEW_SEC", "15", cast=int)  # kiçik saat fərqi buferi
NODE_REDEEM_BEARER       = env("NODE_REDEEM_BEARER", "")               # lazım deyilsə boş qalsın


TEKUIS_VALIDATION_MIN_OVERLAP_SQM = 0.25   # çox xırda sliver-lər itməsin deyirsənsə 0.01 də verə bilərsən
TEKUIS_VALIDATION_MIN_GAP_SQM     = 5.0


# ======================
# MSSQL (CRRS_DB) konfiqurasiyası (hamısı .env-dən)
# ======================
# (CRRS_* açarları ilə geriyə-uyğunluq saxlanılıb)
MSSQL_HOST       = env('MSSQL_HOST',       env('CRRS_MSSQL_HOST',       ""))
MSSQL_PORT       = env('MSSQL_PORT',       env('CRRS_MSSQL_PORT',       "1433"), cast=int)
MSSQL_NAME       = env('MSSQL_NAME',       env('CRRS_MSSQL_DB',         ""))
MSSQL_USER       = env('MSSQL_USER',       env('CRRS_MSSQL_USER',       ""))
MSSQL_PASSWORD   = env('MSSQL_PASSWORD',   env('CRRS_MSSQL_PASSWORD',   ""))
MSSQL_DRIVER     = env('MSSQL_DRIVER',     env('CRRS_MSSQL_DRIVER',     "ODBC Driver 18 for SQL Server"))
MSSQL_ENCRYPT    = env_bool('MSSQL_ENCRYPT',    env('CRRS_MSSQL_ENCRYPT',  "false"))
MSSQL_TRUST_CERT = env_bool('MSSQL_TRUST_CERT', env('CRRS_MSSQL_TRUST_CERT', "true"))
MSSQL_TIMEOUT    = env('MSSQL_TIMEOUT',    env('CRRS_MSSQL_TIMEOUT',    "5"), cast=int)
MSSQL_SCHEMA     = env('MSSQL_SCHEMA', 'dbo')

# ======================
# ATTACH / SMB konfiqurasiyası (hamısı .env-dən)
# ======================
ATTACH_BASE_DIR      = env("ATTACH_BASE_DIR", str(BASE_DIR / "attach_local"))  # UNC və ya lokal
ATTACH_FALLBACK_DIR  = env("ATTACH_FALLBACK_DIR", str(BASE_DIR / "attach_local"))
ATTACH_FORCE_LOCAL   = env_bool("ATTACH_FORCE_LOCAL", False)

ATTACH_SMB_HOST      = env("ATTACH_SMB_HOST", "")
ATTACH_SMB_SHARE     = env("ATTACH_SMB_SHARE", "")
ATTACH_SMB_DOMAIN    = env("ATTACH_SMB_DOMAIN", "")
ATTACH_SMB_USER      = env("ATTACH_SMB_USER", "")
ATTACH_SMB_PASSWORD  = env("ATTACH_SMB_PASSWORD", "")





LOGGING = {
  "version": 1,
  "disable_existing_loggers": False,
  "formatters": {
    "short": {"format": "[%(asctime)s] %(levelname)s %(name)s: %(message)s"}
  },
  "handlers": {
    "console": {"class": "logging.StreamHandler", "formatter": "short"}
  },
  "loggers": {
    "corrections": {"handlers": ["console"], "level": "INFO"},
  }
}
