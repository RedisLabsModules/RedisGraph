/*
 * Copyright 2018-2020 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include <string.h>
#include "../deps/GraphBLAS/Source/GB_assert.h"
#include "../configuration/config.h"

void _Config_get_all(RedisModuleCtx *ctx) {
	uint config_count = Config_END_MARKER;
	RedisModule_ReplyWithArray(ctx, config_count);

	for(Config_Option_Field field = 0; field < Config_END_MARKER; field++) {
		long long value = 0;
		const char *config_name = Config_Field_name(field);
		bool res = Config_Option_get(field, &value);

		if(!res || config_name == NULL) {
			RedisModule_ReplyWithError(ctx, "Configuration field was not found");
			return;
		} else {
			RedisModule_ReplyWithArray(ctx, 2);
			RedisModule_ReplyWithCString(ctx, config_name);
			RedisModule_ReplyWithLongLong(ctx, value);
		}
	}
}

void _Config_get(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	// return the given config's value to the user
	Config_Option_Field config_field;
	const char *config_name = RedisModule_StringPtrLen(argv[2], NULL);

	// return entire configuration
	if(!strcmp(config_name, "*")) {
		_Config_get_all(ctx);
		return;
	}

	// return specific configuration field
	if(!Config_Contains_field(config_name, &config_field)) {
		RedisModule_ReplyWithError(ctx, "Unknown configuration field");
		return;
	}

	long long value = 0;

	if(Config_Option_get(config_field, &value)) {
		RedisModule_ReplyWithArray(ctx, 2);
		RedisModule_ReplyWithCString(ctx, config_name);
		RedisModule_ReplyWithLongLong(ctx, value);
	} else {
		RedisModule_ReplyWithError(ctx, "Configuration field was not found");
	}
}

bool _Config_set(RedisModuleCtx *ctx, RedisModuleString *key,
		RedisModuleString *val) {
	//--------------------------------------------------------------------------
	// retrieve and validate config field
	//--------------------------------------------------------------------------

	Config_Option_Field config_field;
	const char *config_name = RedisModule_StringPtrLen(key, NULL);

	if(!Config_Contains_field(config_name, &config_field)) {
		RedisModule_ReplyWithError(ctx, "Unknown configuration field");
		return true;
	}

	// ensure field is whitelisted
	bool configurable_field = false;
	for(int i = 0; i < RUNTIME_CONFIG_COUNT; i++) {
		if(RUNTIME_CONFIGS[i] == config_field) {
			configurable_field = true;
			break;
		}
	}
	
	// field is not allowed to be reconfigured
	if(!configurable_field) {
		RedisModule_ReplyWithError(ctx, "Field can not be re-configured");
		return true;
	}

	// set the value of given config
	const char *val_str = RedisModule_StringPtrLen(val, NULL);

	if(Config_Option_set(config_field, val_str)) {
		RedisModule_ReplyWithSimpleString(ctx, "OK");
	} else {
		RedisModule_ReplyWithError(ctx, "Failed to set config value");
		return true;
	}

	return false;
}

int _Config_array_set(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool dryrun) {
	// To ensure atomicity we check if configuration can be setted.
	if(dryrun) {
		Config_Clone();                 // Clone the config for the case of error.
		Config_Unsubscribe_Changes();   // Unsubscribe from configuration changes to avoid propagating the changes.
	}

	// set configuration for each requested one
	for(int i = 2; i < argc; i += 2) {
		RedisModuleString *key = argv[i];
		RedisModuleString *val = argv[i+1];
		if(_Config_set(ctx, key, val)) {
			// On error quit the operation.
			ASSERT(dryrun);
			return -1;
		}
	}

	if(dryrun) Config_RestoreFromClone();
	return 0;
}

int Graph_Config(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	// GRAPH.CONFIG <GET|SET> <NAME> [value]
	if(argc < 3) return RedisModule_WrongArity(ctx);

	const char *action = RedisModule_StringPtrLen(argv[1], NULL);

	if(!strcasecmp(action, "GET")) {
		// GRAPH.CONFIG GET <NAME>
		if(argc != 3) return RedisModule_WrongArity(ctx);
		_Config_get(ctx, argv, argc);
	} else if(!strcasecmp(action, "SET")) {
		// GRAPH.CONFIG SET <NAME> [value] <NAME> [value] ...
		// emit an error if we received an odd number of arguments,
		// as this indicates an invalid configuration
		if(argc < 4 || (argc % 2) == 1) return RedisModule_WrongArity(ctx);

		// To ensure atomicity first check if configuration can be setted and dryrun the configuration.
		if(_Config_array_set(ctx, argv, argc, true)) return REDISMODULE_OK;
		if(_Config_array_set(ctx, argv, argc, false)) ASSERT(false);
	} else {
		RedisModule_ReplyWithError(ctx, "Unknown subcommand for GRAPH.CONFIG");
	}

	return REDISMODULE_OK;
}

