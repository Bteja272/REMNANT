#include <algorithm>
#include <iostream>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

struct WorldFlag {
    std::string flagKey;
    bool flagValue;
    std::string description;
};

struct NpcMemory {
    bool exists = false;
    std::string memoryType;
    std::string description;
    int intensity = 0;
};

std::string readStdin() {
    std::ostringstream buffer;
    buffer << std::cin.rdbuf();
    return buffer.str();
}

std::string toLower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return std::tolower(c);
    });
    return value;
}

std::string jsonEscape(const std::string& value) {
    std::string escaped;

    for (char c : value) {
        switch (c) {
            case '"':
                escaped += "\\\"";
                break;
            case '\\':
                escaped += "\\\\";
                break;
            case '\n':
                escaped += "\\n";
                break;
            case '\r':
                escaped += "\\r";
                break;
            case '\t':
                escaped += "\\t";
                break;
            default:
                escaped += c;
        }
    }

    return escaped;
}

std::string extractStringField(const std::string& json, const std::string& fieldName) {
    std::regex pattern("\"" + fieldName + "\"\\s*:\\s*\"([^\"]*)\"");
    std::smatch match;

    if (std::regex_search(json, match, pattern)) {
        return match[1].str();
    }

    return "";
}

int getBaseReputationDelta(const std::string& actionType) {
    if (actionType == "HELP_FACTION") {
        return 15;
    }

    if (actionType == "DONATE_RESOURCE") {
        return 10;
    }

    if (actionType == "ROB_NPC") {
        return -20;
    }

    if (actionType == "ATTACK_FACTION") {
        return -25;
    }

    if (actionType == "SPARE_ENEMY") {
        return 8;
    }

    return 0;
}

NpcMemory buildNpcMemory(
    const std::string& actionType,
    const std::string& targetFaction,
    const std::string& npcId,
    const std::string& resource
) {
    NpcMemory memory;

    if (npcId.empty()) {
        return memory;
    }

    memory.exists = true;

    if (actionType == "HELP_FACTION") {
        memory.memoryType = "HELPED_ALLIED_FACTION";
        memory.description = "Player helped " + (targetFaction.empty() ? std::string("an allied faction") : targetFaction) + ".";
        memory.intensity = 6;
        return memory;
    }

    if (actionType == "DONATE_RESOURCE") {
        memory.memoryType = "DONATED_RESOURCE";
        memory.description = "Player donated " + (resource.empty() ? std::string("resources") : resource) + ".";
        memory.intensity = 7;
        return memory;
    }

    if (actionType == "ROB_NPC") {
        memory.memoryType = "ROBBED_BY_PLAYER";
        memory.description = "Player robbed this NPC during an encounter.";
        memory.intensity = 9;
        return memory;
    }

    if (actionType == "SPARE_ENEMY") {
        memory.memoryType = "SPARED_BY_PLAYER";
        memory.description = "Player spared this NPC instead of killing or capturing them.";
        memory.intensity = 8;
        return memory;
    }

    if (actionType == "ATTACK_FACTION") {
        memory.memoryType = "ATTACKED_FACTION";
        memory.description = "Player attacked " + (targetFaction.empty() ? std::string("this NPC's faction") : targetFaction) + ".";
        memory.intensity = 8;
        return memory;
    }

    memory.exists = false;
    return memory;
}

std::vector<WorldFlag> buildWorldFlags(
    const std::string& actionType,
    const std::string& targetFaction,
    const std::string& npcId,
    const std::string& resource
) {
    std::vector<WorldFlag> flags;

    const std::string faction = toLower(targetFaction);
    const std::string npc = toLower(npcId);
    const std::string normalizedResource = toLower(resource);

    if (
        actionType == "DONATE_RESOURCE" &&
        faction == "survivors" &&
        normalizedResource == "medicine"
    ) {
        flags.push_back({
            "survivors_clinic_supplied",
            true,
            "Dusthaven Clinic has enough medicine because the player donated supplies to the Survivors."
        });
    }

    if (actionType == "ROB_NPC" && npc == "mara") {
        flags.push_back({
            "trader_market_unstable",
            true,
            "Mara's trade post is unstable after the player robbed her."
        });
    }

    if (actionType == "SPARE_ENEMY" && npc == "knox") {
        flags.push_back({
            "raider_checkpoint_ambush_disabled",
            true,
            "Knox remembers being spared, reducing the chance of a Raider checkpoint ambush."
        });
    }

    if (actionType == "ATTACK_FACTION" && faction == "raiders") {
        flags.push_back({
            "raider_checkpoint_hostile",
            true,
            "The Raider checkpoint has become hostile after the player attacked Raiders."
        });
    }

    return flags;
}

int main() {
    const std::string input = readStdin();

    const std::string eventId = extractStringField(input, "eventId");
    const std::string playerId = extractStringField(input, "playerId");
    const std::string actionType = extractStringField(input, "actionType");
    const std::string targetFaction = extractStringField(input, "targetFaction");
    const std::string npcId = extractStringField(input, "npcId");
    const std::string resource = extractStringField(input, "resource");

    const int directReputationDelta = getBaseReputationDelta(actionType);

    const NpcMemory npcMemory = buildNpcMemory(
        actionType,
        targetFaction,
        npcId,
        resource
    );

    const std::vector<WorldFlag> worldFlags = buildWorldFlags(
        actionType,
        targetFaction,
        npcId,
        resource
    );

    std::cout << "{";
    std::cout << "\"engine\":\"cpp\",";
    std::cout << "\"engineVersion\":\"0.1.0\",";
    std::cout << "\"eventId\":\"" << jsonEscape(eventId) << "\",";
    std::cout << "\"playerId\":\"" << jsonEscape(playerId) << "\",";
    std::cout << "\"actionType\":\"" << jsonEscape(actionType) << "\",";
    std::cout << "\"directReputationDelta\":" << directReputationDelta << ",";

    std::cout << "\"npcMemory\":";
    if (npcMemory.exists) {
        std::cout << "{";
        std::cout << "\"memoryType\":\"" << jsonEscape(npcMemory.memoryType) << "\",";
        std::cout << "\"description\":\"" << jsonEscape(npcMemory.description) << "\",";
        std::cout << "\"intensity\":" << npcMemory.intensity;
        std::cout << "}";
    } else {
        std::cout << "null";
    }

    std::cout << ",";

    std::cout << "\"worldFlags\":[";
    for (size_t i = 0; i < worldFlags.size(); ++i) {
        const WorldFlag& flag = worldFlags[i];

        std::cout << "{";
        std::cout << "\"flagKey\":\"" << jsonEscape(flag.flagKey) << "\",";
        std::cout << "\"flagValue\":" << (flag.flagValue ? "true" : "false") << ",";
        std::cout << "\"description\":\"" << jsonEscape(flag.description) << "\"";
        std::cout << "}";

        if (i + 1 < worldFlags.size()) {
            std::cout << ",";
        }
    }
    std::cout << "]";

    std::cout << "}";

    return 0;
}