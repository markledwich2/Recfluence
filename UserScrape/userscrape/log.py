import seqlog
import logging
import json


def configure_log(url: str, env='dev', branch_env=None, trial_id=None) -> logging.Logger:
    seqlog.configure_from_dict({
        'version': 1,
        'disable_existing_loggers': True,
        'root': {
            'level': 'WARN',
            'handlers': ['console']
        },
        'loggers': {
            'seq': {
                'level': 'DEBUG',
                'handlers': ['seq'],
                'propagate': True
            }
        },
        'handlers': {
            'console': {
                'class': 'seqlog.structured_logging.ConsoleStructuredLogHandler',
                'formatter': 'seq'
            },
            'seq': {
                'class': 'seqlog.structured_logging.SeqLogHandler',
                'server_url': url,
                'batch_size': 10,
                'auto_flush_timeout': 2,
                'formatter': 'seq'
            }
        },
        'formatters': {
            'seq': {
                'style': '{'
            }
        }
    })

    seqlog.set_global_log_properties(
        app="UserScrape",
        env=env,
        branch_env=branch_env,
        Trial=trial_id
    )

    return logging.getLogger('seq')
