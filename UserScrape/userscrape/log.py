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
                'level': 'INFO',
                'handlers': ['seq']
            }
        },
        'handlers': {
            'console': {
                'class': 'seqlog.structured_logging.ConsoleStructuredLogHandler'
            },
            'seq': {
                'class': 'seqlog.structured_logging.SeqLogHandler',
                'server_url': url,
                'batch_size': 1
            }
        }
    })

    seqlog.set_global_log_properties(
        app="UserScrape",
        env=env,
        branch_env=branch_env,
        trial_id=trial_id
    )

    return logging.getLogger('seq')
