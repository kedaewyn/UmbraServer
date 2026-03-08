<p align="center">
  <img src="https://repo.ashfall-codex.dev/img/umbra-full.png" alt="UmbraSync Server" width="128" />
</p>

<h1 align="center">UmbraSync Server</h1>

<p align="center">
  <b>Serveur relay temps-réel pour FFXIV</b> : synchronisation de personnages, Syncshells, partage de mods et profils RP entre joueurs via SignalR.
</p>

<p align="center">
  API <code>v3000</code> &middot; .NET 10 &middot; PostgreSQL &middot; Redis &middot; SignalR + MessagePack
</p>

---

## Vue d'ensemble

UmbraSync Server est le backend du plugin [UmbraSync](https://github.com/Ashfall-Codex/UmbraClient). Il gère la synchronisation temps-réel des données de personnages, l'authentification, la distribution de fichiers et les services annexes (bot Discord). Le système supporte le scaling horizontal via une architecture main/shard.

---

## Architecture

Le serveur est composé de 5 projets .NET et d'une bibliothèque de contrats partagée :

| Projet | Rôle |
|---|---|
| **UmbraSyncServer** | Hub SignalR principal (`/mare`) : synchronisation temps-réel, gestion des paires, Syncshells, profils, MCDF, housing, quêtes |
| **UmbraSyncAuthService** | Authentification JWT, enregistrement de comptes, rate limiting, géo-IP, discovery |
| **UmbraSyncStaticFilesServer** | CDN et distribution de fichiers : cache, upload/download, stockage cloud (Scaleway) |
| **UmbraSyncServices** | Services background : bot Discord (commandes, notifications) |
| **UmbraSyncShared** | Bibliothèque partagée : DbContext EF Core, 21 entités, configuration, métriques Prometheus |
| **[UmbraAPI](https://github.com/Ashfall-Codex/UmbraAPI)** | Contrats partagés : interfaces SignalR, 57 DTOs, enums, routes HTTP |

### Main / Shard

- **Mode main** (`MainServerAddress` = null) : exécute les migrations, les services de nettoyage et sert de source de configuration.
- **Mode shard** (`MainServerAddress` = URL du main) : synchronise sa configuration depuis le main via les propriétés `[RemoteConfiguration]`, se connecte au même Redis backplane.

---

## Technologies

| Composant | Technologie |
|---|---|
| Runtime | .NET 10 / ASP.NET Core |
| Temps-réel | SignalR + MessagePack + compression LZ4 |
| Base de données | PostgreSQL + EF Core (snake_case) |
| Cache & backplane | Redis (caching, SignalR backplane, présence) |
| Authentification | JWT Bearer |
| Métriques | Prometheus |
| Stockage fichiers | Scaleway Object Storage |
| Bot | Discord.Net |

---

## Structure du projet

```
UmbraSyncServer/                    # Hub SignalR principal
├── Hubs/                           # 16 partial classes du hub + 2 filtres
│   ├── MareHub.cs                  # Connexion, authentification
│   ├── MareHub.User.cs             # Paires, profils, push de données
│   ├── MareHub.Groups.cs           # Gestion des Syncshells
│   ├── MareHub.CharaData.cs        # Données personnage (MCDF)
│   ├── MareHub.Slots.cs            # Groupes par localisation
│   ├── MareHub.GposeLobby.cs       # Gpose Together
│   ├── MareHub.Chat.cs             # Messagerie
│   ├── MareHub.McdfShare.cs        # Partage de mods chiffré
│   ├── MareHub.Typing.cs           # Indicateurs de saisie
│   ├── MareHub.QuestSync.cs        # Synchronisation de quêtes
│   ├── MareHub.HousingShare.cs     # Partage de housing
│   ├── MareHub.Rgpd.cs             # Export/suppression RGPD
│   └── ...
├── Controllers/                    # 3 contrôleurs REST
├── Services/                       # 8 services (cleanup, cache, AutoDetect)
└── Startup.cs                      # Policies d'autorisation

UmbraSyncAuthService/               # Authentification
├── Controllers/                    # JWT, WellKnown, Discovery
├── Services/                       # 6 services (registration, auth, GeoIP)
└── Authentication/                 # DTOs d'authentification

UmbraSyncStaticFilesServer/         # CDN / Distribution de fichiers
├── Controllers/                    # 6 contrôleurs (cache, distribution, upload)
├── Services/                       # 10 services (queue, prefetch, Scaleway)
└── Utils/                          # Streams, hashing, file paths

UmbraSyncServices/                  # Services background
└── Discord/                        # Bot Discord (commandes, notifications)

UmbraSyncShared/                    # Bibliothèque partagée
├── Data/
│   └── MareDbContext.cs            # DbContext EF Core
├── Models/                         # 21 entités (User, ClientPair, Group, CharaData, etc.)
├── Migrations/                     # 34 migrations
├── Utils/
│   └── Configuration/              # 8 classes de configuration
├── Services/                       # Configuration main/shard
├── RequirementHandlers/            # Policies d'autorisation
├── Metrics/                        # Prometheus
└── Extensions.cs                   # Résolution d'IP, helpers
```

---

## SignalR Hub

**Path** : `/mare` &middot; **API** : `v3000` &middot; **Sérialisation** : MessagePack + LZ4

Le hub implémente `IMareHub` (88 méthodes serveur) et envoie des callbacks via `IMareHubClient` (30 méthodes client).

| Domaine | Exemples de méthodes |
|---|---|
| Paires | `UserAddPair`, `UserRemovePair`, `UserPushData`, `UserSetPairPermissions` |
| Groupes | `GroupCreate`, `GroupJoin`, `GroupLeave`, `GroupDelete`, `GroupPrune` |
| Profils | `UserSetProfile`, `UserGetProfile`, `UserGetAllCharacterProfiles` |
| CharaData | `CharaDataCreate`, `CharaDataUpdate`, `CharaDataDownload` |
| Typing | `UserSetTypingState`, `UserUpdateTypingChannels` |
| Gpose | `GposeLobbyCreate`, `GposeLobbyJoin`, `GposeLobbyPushPoseData` |
| Discovery | `SyncshellDiscoveryList`, `SyncshellDiscoveryJoin` |
| Slots | `SlotGetInfo`, `SlotGetNearby`, `SlotUpdate`, `SlotJoin` |
| Housing | `HousingShareUpload`, `HousingShareDownload` |
| Quêtes | `QuestSessionCreate`, `QuestSessionJoin`, `QuestSessionPushState` |
| RGPD | `UserRgpdExportData`, `UserRgpdDeleteAllData` |

---

## Modèle de données

### Entités principales

| Entité | Description |
|---|---|
| `User` | Utilisateur (UID 10 chars, alias optionnel, charaIdent) |
| `Auth` | Clé secrète hashée, lien vers User |
| `ClientPair` | Relation de synchronisation 1-to-1 directionnelle |
| `Group` | Syncshell (GID 20 chars, owner, password, capacité max, temporaire) |
| `GroupPair` | Membre d'une Syncshell avec permissions |
| `Slot` | Localisation physique FFXIV liée à un groupe |
| `CharaData` | Données d'apparence uploadées avec contrôle d'accès |
| `UserProfileData` | Profil HRP (avatar, description) |
| `CharacterRpProfileData` | Profil RP par personnage (prénom, titre, race, etc.) |
| `HousingShare` | Layout de housing partagé |
| `McdfShare` | Fichier mod chiffré partagé |
| `AutoDetectSchedule` | Planification de la détection automatique |
| `FileCache` | Métadonnées des fichiers en cache |

---

## Policies d'autorisation

| Policy | Condition |
|---|---|
| **Authenticated** | JWT valide |
| **Identified** | JWT + utilisateur existant en base |
| **Admin** | Identified + `IsAdmin` |
| **Moderator** | Identified + `IsModerator` ou `IsAdmin` |
| **Internal** | Communication inter-services |

---

## Build

### Prérequis

- .NET 10.0 SDK
- PostgreSQL
- Redis

### Compilation

```bash
# Build de la solution complète
dotnet build UmbraSyncServer/UmbraSync.sln

# Build d'un projet spécifique
dotnet build UmbraSyncServer/UmbraSyncServer/UmbraSyncServer.csproj
dotnet build UmbraSyncServer/UmbraSyncAuthService/UmbraSyncAuthService.csproj
dotnet build UmbraSyncServer/UmbraSyncStaticFilesServer/UmbraSyncStaticFilesServer.csproj
dotnet build UmbraSyncServer/UmbraSyncServices/UmbraSyncServices.csproj
```

### Lancement

```bash
# Serveur principal (hub SignalR)
dotnet run --project UmbraSyncServer/UmbraSyncServer/UmbraSyncServer.csproj

# Service d'authentification
dotnet run --project UmbraSyncServer/UmbraSyncAuthService/UmbraSyncAuthService.csproj

# Serveur de fichiers statiques (CDN)
dotnet run --project UmbraSyncServer/UmbraSyncStaticFilesServer/UmbraSyncStaticFilesServer.csproj

# Services background (bot Discord)
dotnet run --project UmbraSyncServer/UmbraSyncServices/UmbraSyncServices.csproj
```

### Migrations EF Core

```bash
# Ajouter une migration
dotnet ef migrations add <Nom> --project UmbraSyncServer/UmbraSyncShared --startup-project UmbraSyncServer/UmbraSyncServer

# Appliquer les migrations (automatique au démarrage du serveur principal)
dotnet ef database update --project UmbraSyncServer/UmbraSyncShared --startup-project UmbraSyncServer/UmbraSyncServer
```

---

## Configuration

Chaque service utilise un fichier `appsettings.json` avec la section `MareSynchronos`. Les classes de configuration se trouvent dans `UmbraSyncShared/Utils/Configuration/`.

| Classe | Usage |
|---|---|
| `MareConfigurationBase` | Commun : pool DB, JWT, Redis, port métriques |
| `ServerConfiguration` | Hub : URL CDN, limites groupes, purge |
| `AuthServiceConfiguration` | Auth : paramètres spécifiques |
| `StaticFilesServerConfiguration` | CDN : stockage, cache |
| `ServicesConfiguration` | Bot Discord |

Les propriétés marquées `[RemoteConfiguration]` sont synchronisées automatiquement du main vers les shards.

---

## Projets liés

| Projet | Description |
|---|---|
| [UmbraSync (Client)](https://github.com/Ashfall-Codex/UmbraClient) | Plugin Dalamud pour FFXIV |
| [UmbraSync API](https://github.com/Ashfall-Codex/UmbraAPI) | Contrats partagés (DTOs, interfaces SignalR) |

---

## Licence

Le code original est sous licence MIT (voir `LICENSE_MIT`). A partir du commit `46f2443`, le code est sous licence **AGPL v3** (voir `LICENSE`).