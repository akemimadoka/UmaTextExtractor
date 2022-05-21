#include <sqlite3.h>

#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <unordered_map>

#include <Cafe/TextUtils/Misc.h>

#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/writer.h>

int main(int argc, char** argv)
{
	if (argc != 3 && argc != 4)
	{
		std::cout << "Usage: " << argv[0] << " <master database file> <output file> [hash path]"
		          << std::endl;
		return 1;
	}

	const auto masterDbPath = argv[1];
	const std::filesystem::path outputPath = argv[2];
	std::filesystem::create_directories(outputPath);

	std::unordered_map<std::size_t, std::string> hashMap;
	if (argc == 4)
	{
		const std::filesystem::path hashPath = argv[3];

		for (const auto& entry : std::filesystem::recursive_directory_iterator(hashPath))
		{
			if (!entry.is_regular_file())
			{
				continue;
			}

			std::ifstream input(entry.path());

			rapidjson::IStreamWrapper wrapper(input);
			rapidjson::Document document;
			document.ParseStream(wrapper);

			if (document.HasParseError() || !document.IsObject())
			{
				std::cout << "Skip malformed file " << entry.path() << std::endl;
				continue;
			}

			for (const auto& [hash, str] : document.GetObject())
			{
				const std::string_view hashStr = hash.GetString();
				const auto strStr = str.GetString();

				std::size_t hashValue;
				if (const auto [ptr, ec] =
				        std::from_chars(hashStr.data(), hashStr.data() + hashStr.size(), hashValue);
				    ptr != hashStr.data() + hashStr.size() || ec != std::errc())
				{
					std::cout << "Skip malformed or static dict file " << entry.path() << std::endl;
					goto NextFile;
				}

				hashMap.emplace(hashValue, strStr);
			}

		NextFile:;
		}
	}

	const auto mayGetLocalizedText = [&](const char* text) -> const std::string* {
		if (!hashMap.empty())
		{
			const auto textView =
			    Cafe::Encoding::StringView<Cafe::Encoding::CodePage::Utf8>::FromNullTerminatedStr(
			        reinterpret_cast<const char8_t*>(text));
			const auto wideStr = Cafe::TextUtils::EncodeToWide(textView);
			const auto hash = std::hash<std::wstring>{}(wideStr);
			if (const auto it = hashMap.find(hash); it != hashMap.end())
			{
				return &it->second;
			}
		}

		return nullptr;
	};

	sqlite3* db;
	if (!sqlite3_open(masterDbPath, &db))
	{
		std::cout << "Opened master database file: " << masterDbPath << std::endl;

		sqlite3_stmt* stmt;
		if (!sqlite3_prepare_v2(db, "select category, [index], text from text_data", -1, &stmt,
		                        nullptr))
		{
			std::unordered_map<std::size_t, std::unordered_map<std::size_t, std::string>>
			    textDataMap;

			while (sqlite3_step(stmt) == SQLITE_ROW)
			{
				const auto category = sqlite3_column_int64(stmt, 0);
				const auto index = sqlite3_column_int64(stmt, 1);
				const auto text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
				if (const auto localizedText = mayGetLocalizedText(text))
				{
					textDataMap[category].emplace(index, *localizedText);
				}
				else
				{
					textDataMap[category].emplace(index, text);
				}
			}

			sqlite3_finalize(stmt);

			std::ofstream output(outputPath / "text_data.json");

			rapidjson::OStreamWrapper wrapper(output);
			rapidjson::Writer writer(wrapper);

			writer.StartObject();

			for (const auto& [category, textData] : textDataMap)
			{
				writer.Key(std::to_string(category).c_str());
				writer.StartObject();

				for (const auto& [index, text] : textData)
				{
					writer.Key(std::to_string(index).c_str());
					writer.String(text.data(), text.size());
				}

				writer.EndObject();
			}

			writer.EndObject();
		}
		else
		{
			std::cout << "Failed to prepare statement: " << sqlite3_errmsg(db) << std::endl;
		}

		if (!sqlite3_prepare_v2(db,
		                        "select character_id, voice_id, text from character_system_text",
		                        -1, &stmt, nullptr))
		{
			std::unordered_map<std::size_t, std::unordered_map<std::size_t, std::string>>
			    characterSystemTextMap;

			while (sqlite3_step(stmt) == SQLITE_ROW)
			{
				const auto characterId = sqlite3_column_int64(stmt, 0);
				const auto voiceId = sqlite3_column_int64(stmt, 1);
				const auto text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
				if (const auto localizedText = mayGetLocalizedText(text))
				{
					characterSystemTextMap[characterId].emplace(voiceId, *localizedText);
				}
				else
				{
					characterSystemTextMap[characterId].emplace(voiceId, text);
				}
			}

			sqlite3_finalize(stmt);

			std::ofstream output(outputPath / "character_system_text.json");

			rapidjson::OStreamWrapper wrapper(output);
			rapidjson::Writer writer(wrapper);

			writer.StartObject();

			for (const auto& [characterId, textData] : characterSystemTextMap)
			{
				writer.Key(std::to_string(characterId).c_str());
				writer.StartObject();

				for (const auto& [voiceId, text] : textData)
				{
					writer.Key(std::to_string(voiceId).c_str());
					writer.String(text.data(), text.size());
				}

				writer.EndObject();
			}

			writer.EndObject();
		}
		else
		{
			std::cout << "Failed to prepare statement: " << sqlite3_errmsg(db) << std::endl;
		}

		if (!sqlite3_prepare_v2(db, "select id, message from race_jikkyo_comment", -1, &stmt,
		                        nullptr))
		{
			std::unordered_map<std::size_t, std::string> raceJikkyoCommentMap;

			while (sqlite3_step(stmt) == SQLITE_ROW)
			{
				const auto id = sqlite3_column_int64(stmt, 0);
				const auto message = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
				if (const auto localizedText = mayGetLocalizedText(message))
				{
					raceJikkyoCommentMap.emplace(id, *localizedText);
				}
				else
				{
					raceJikkyoCommentMap.emplace(id, message);
				}
			}

			sqlite3_finalize(stmt);

			std::ofstream output(outputPath / "race_jikkyo_comment.json");

			rapidjson::OStreamWrapper wrapper(output);
			rapidjson::Writer writer(wrapper);

			writer.StartObject();

			for (const auto& [id, message] : raceJikkyoCommentMap)
			{
				writer.Key(std::to_string(id).c_str());
				writer.String(message.data(), message.size());
			}

			writer.EndObject();
		}
		else
		{
			std::cout << "Failed to prepare statement: " << sqlite3_errmsg(db) << std::endl;
		}

		if (!sqlite3_prepare_v2(db, "select id, message from race_jikkyo_message", -1, &stmt,
		                        nullptr))
		{
			std::unordered_map<std::size_t, std::string> raceJikkyoMessageMap;

			while (sqlite3_step(stmt) == SQLITE_ROW)
			{
				const auto id = sqlite3_column_int64(stmt, 0);
				const auto text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
				if (const auto localizedText = mayGetLocalizedText(text))
				{
					raceJikkyoMessageMap.emplace(id, *localizedText);
				}
				else
				{
					raceJikkyoMessageMap.emplace(id, text);
				}
			}

			sqlite3_finalize(stmt);

			std::ofstream output(outputPath / "race_jikkyo_message.json");

			rapidjson::OStreamWrapper wrapper(output);
			rapidjson::Writer writer(wrapper);

			writer.StartObject();

			for (const auto& [id, message] : raceJikkyoMessageMap)
			{
				writer.Key(std::to_string(id).c_str());
				writer.String(message.data(), message.size());
			}

			writer.EndObject();
		}
		else
		{
			std::cout << "Failed to prepare statement: " << sqlite3_errmsg(db) << std::endl;
		}
	}
	else
	{
		std::cout << "Failed to open master database file: " << masterDbPath << std::endl;
		return 1;
	}

	return 0;
}
