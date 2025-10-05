# Secure Credential Storage

## Overview

This application implements a secure, cross-platform credential storage system for S3 connection bookmarks. The system separates sensitive data (secret keys) from non-sensitive configuration data for maximum security.

## Architecture

### Two-Tier Storage Model

1. **SQLite Database** - Stores non-sensitive bookmark information:
   - Bookmark title
   - S3 endpoint URL
   - AWS region
   - Access Key ID
   - Session token (optional)
   - Timestamps (created, updated, last used)

2. **OS Keychain/Credential Manager** - Stores sensitive data:
   - AWS Secret Access Key
   - Encrypted and protected by the operating system

### Why This Approach?

This two-tier approach provides several security benefits:

- **OS-Level Encryption**: Secret keys are encrypted using the OS's native encryption mechanisms
- **Access Control**: Secret keys require user authentication (e.g., macOS Keychain password, Windows Credential Manager)
- **Separation of Concerns**: Even if the database file is compromised, secret keys remain secure
- **Cross-Platform**: Works consistently across macOS, Windows, and Linux

## Platform-Specific Implementations

### macOS
- Uses **Keychain Services API**
- Secrets stored in the default keychain (`login.keychain-db`)
- Protected by user's login password
- Can be viewed/managed via Keychain Access.app
- Service name: `stree-s3-browser`

### Windows
- Uses **Windows Credential Manager**
- Secrets stored in Windows Credential Vault
- Protected by Windows Data Protection API (DPAPI)
- Can be viewed/managed via Control Panel → Credential Manager
- Service name: `stree-s3-browser`

### Linux
- Uses **Secret Service API** (freedesktop.org standard)
- Compatible with GNOME Keyring, KDE Wallet, and others
- Requires a secret service daemon to be running
- Secrets are typically encrypted with a master password
- Service name: `stree-s3-browser`

## Usage

### Creating a Bookmark with Secure Credentials

```go
import (
    "context"
    "github.com/vgarvardt/stree/pkg/models"
    "github.com/vgarvardt/stree/pkg/storage"
)

// Initialize storage and credential store
db, _ := storage.New(ctx, storage.Config{DSN: "./storage.db"})
credStore := storage.NewCredentialStore()

// Create a bookmark
bookmark := &models.Bookmark{
    Title:        "Production S3",
    Endpoint:     "https://s3.amazonaws.com",
    Region:       "us-east-1",
    AccessKeyID:  "AKIAIOSFODNN7EXAMPLE",
    SessionToken: "", // Optional
}

// Save bookmark to database
err := db.UpsertBookmark(ctx, bookmark)
// bookmark.ID is now populated

// Store the secret key separately in OS keychain
secretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
err = credStore.StoreSecretKey(ctx, bookmark.ID, secretKey)
```

### Retrieving a Bookmark with Credentials

```go
// Get bookmark from database
bookmark, err := db.GetBookmark(ctx, bookmarkID)

// Retrieve secret key from OS keychain
secretKey, err := credStore.GetSecretKey(ctx, bookmark.ID)

// Now you have all the credentials needed to connect
s3Config := s3client.Config{
    Endpoint:     bookmark.Endpoint,
    Region:       bookmark.Region,
    AccessKey:    bookmark.AccessKeyID,
    SecretKey:    secretKey,
    SessionToken: bookmark.SessionToken,
}
```

### Deleting a Bookmark

When deleting a bookmark, remember to clean up both the database entry and the keychain entry:

```go
// Delete from database
err := db.DeleteBookmark(ctx, bookmarkID)

// Delete secret from keychain
err = credStore.DeleteSecretKey(ctx, bookmarkID)
```

## Security Considerations

### Benefits

1. **No Plaintext Storage**: Secret keys are never stored in plaintext on disk
2. **OS-Level Protection**: Credentials benefit from OS security features:
   - Encryption at rest
   - Access control lists
   - Audit logging (on some platforms)
3. **Memory Protection**: The keyring libraries minimize time secrets spend in memory
4. **User Authentication**: Many platforms require user authentication to access stored secrets

### Limitations

1. **Local Access Only**: If an attacker has full access to your user account while you're logged in, they can potentially access the keychain
2. **No Network Sync**: Keychains are local to each machine (though iCloud Keychain can sync on Apple devices)
3. **Platform Dependencies**: Requires OS keychain services to be available and functioning

### Best Practices

1. **Always Clean Up**: When deleting bookmarks, also delete the associated keychain entries
2. **Test Keychain Access**: Use `credStore.TestKeychain()` to verify keychain availability before storing secrets
3. **Handle Errors Gracefully**: Keychain access can fail (permissions, service unavailable), always handle errors
4. **Don't Log Secrets**: Never log secret keys or credential data
5. **Use Session Tokens**: When possible, use temporary session tokens instead of long-lived credentials

## Troubleshooting

### macOS

**Problem**: "The user name or passphrase you entered is not correct"
- **Solution**: Your macOS keychain may be locked. Unlock it via Keychain Access.app

**Problem**: "Code signing error" or access denied
- **Solution**: Ensure the application is properly code-signed if distributing

### Windows

**Problem**: "Access denied" errors
- **Solution**: Ensure Windows Credential Manager service is running
- **Solution**: Check that UAC isn't blocking access

### Linux

**Problem**: "Cannot connect to secret service"
- **Solution**: Ensure a secret service daemon is installed and running:
  ```bash
  # For GNOME
  sudo apt-get install gnome-keyring
  
  # For KDE
  sudo apt-get install kwalletmanager
  ```

**Problem**: "Failed to unlock collection"
- **Solution**: You may need to set up a keyring password first

## Alternative: File-Based Encryption (Future Enhancement)

For environments where OS keychain is unavailable, consider implementing a file-based encryption approach:

```go
// Potential future implementation
type FileCredentialStore struct {
    encryptionKey []byte // Derived from user password
    filepath      string
}

// Encrypt/decrypt credentials in a local file
```

However, this approach requires:
- User to enter a master password on each app start
- Secure key derivation (e.g., PBKDF2, Argon2)
- Careful handling of encryption keys in memory

The OS keychain approach is preferred as it leverages battle-tested OS security features.

## References

- [go-keyring library](https://github.com/zalando/go-keyring) - Cross-platform keyring access
- [macOS Keychain Services](https://developer.apple.com/documentation/security/keychain_services)
- [Windows Credential Manager](https://docs.microsoft.com/en-us/windows/win32/secauthn/credentials-management)
- [Secret Service API (Linux)](https://specifications.freedesktop.org/secret-service/)

