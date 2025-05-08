/* CasualOS is a set of web-based tools designed to facilitate the creation of real-time, multi-user, context-aware interactive experiences.
 *
 * Copyright (c) 2019-2025 Casual Simulation, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
import type { BotHelper, BotWatcher } from '@casual-simulation/aux-vm/managers';
import type { SubscriptionLike } from 'rxjs';
import { Subject } from 'rxjs';

export interface SymLinkManagerConfig {
    symLinkProvider: SymLinkProvider;
    botWatcher: BotWatcher;
    botHelper: BotHelper;
}

type SupportedFileChangeEvents = 'update' | 'create' | 'delete';

export interface FileChangeEvent {
    host: 'native' | 'external';
    type: SupportedFileChangeEvents;
    uri: string;
    data?: any;
}

export interface SymChangeEvent {
    type: 'readyState';
    data?: any;
}

export default class SymLinkManager {
    private _fileChangeSubject = new Subject<FileChangeEvent>();
    private _symChangeSubject = new Subject<SymChangeEvent>();
    private _enabled: boolean = true;
    private _botWatcher: BotWatcher;
    private _botHelper: BotHelper;
    private _symProvider: SymLinkProvider;
    /**
     * Future-proofing for the future when subscriptions are added to the manager.
     */
    closed: boolean = false;
    private _subs: SubscriptionLike[] = [];

    constructor(config: SymLinkManagerConfig) {
        this._botWatcher = config.botWatcher;
        this._botHelper = config.botHelper;
        this._symProvider = config.symLinkProvider;
        this.fileChange$.subscribe((event) => {
            if (event.host === 'native') {
                switch (event.type) {
                    case 'create':
                        this._symProvider.create(event.uri);
                        break;
                    case 'update':
                        this._symProvider.write(event.uri, event.data);
                        break;
                    case 'delete':
                        this._symProvider.delete(event.uri);
                        break;
                    default:
                        break;
                }
            }
        });
    }

    get fileChange$() {
        return this._fileChangeSubject.asObservable();
    }

    get symChange$() {
        return this._symChangeSubject.asObservable();
    }

    get enabled(): boolean {
        return this._enabled && this._symProvider.ready;
    }
    set enabled(value: boolean) {
        this._enabled = value && this._symProvider.ready;
        this._symChangeSubject.next({
            type: 'readyState',
            data: this._enabled,
        });
        //TODO: Cleanup of subscriptions and handlers
    }

    async userTriggeredInit() {
        if (this._symProvider instanceof FileHandleSymLinkProvider) {
            await this._symProvider.requestDirectoryHandle();
            await this.listenForExternalFileChanges();
        }
    }

    async onFileChange(event: Omit<FileChangeEvent, 'host'>) {
        if (this._enabled) {
            this._fileChangeSubject.next({ ...event, host: 'native' });
        }
    }

    async listenForExternalFileChanges() {
        if (this._symProvider instanceof FileHandleSymLinkProvider) {
            this._symProvider.onExternalFileChanges(this._fileChangeSubject);
        }
    }
    /**
     * Unsubscribes from all subscriptions and marks the manager as closed.
     */
    unsubscribe(): void {
        if (!this.closed) {
            this.closed = true;
            this._subs.forEach((s) => s.unsubscribe());
            this._subs = null;
        }
    }
}

export interface SymLinkProvider {
    ready: boolean;
    create(uri: string): void;
    read(uri: string): Promise<string>;
    write(uri: string, data: any): void;
    delete(uri: string): void;
    onExternalFileChanges(subject: Subject<FileChangeEvent>): void;
}

export class FileHandleSymLinkProvider implements SymLinkProvider {
    private _symLinks: Map<string, string>;
    private _dirHandle: FileSystemDirectoryHandle | null;
    private _fileHandles: Map<string, FileSystemFileHandle>;
    private _mostRecentUri: string | null;
    private _pollingInterval: number;

    /**
     * Gets and caches the directory handle for the file system.
     */
    private async _getDirHandle() {
        if (
            'showDirectoryPicker' in window &&
            typeof window.showDirectoryPicker === 'function'
        ) {
            this._dirHandle = await window.showDirectoryPicker();
            return this._dirHandle;
        } else {
            console.error(
                'showDirectoryPicker is not supported in this browser.'
            );
        }
        return null;
    }

    formatUri(uri: string): string {
        // Format the URI to a valid file name
        return uri
            .replaceAll('/', '_')
            .replaceAll('\\', '_')
            .replaceAll(':', '-');
    }

    constructor() {
        this._symLinks = new Map<string, string>();
        this._fileHandles = new Map<string, FileSystemFileHandle>();
        this._mostRecentUri = null;
        this._dirHandle = null;
    }

    get ready(): boolean {
        return (this._dirHandle ?? null) !== null;
    }

    async requestDirectoryHandle() {
        await this._getDirHandle();
    }

    async create(uri: string): Promise<void> {
        uri = this.formatUri(uri);
        const fHandle = await this._dirHandle.getFileHandle(uri, {
            create: true,
        });
        if (fHandle) {
            this._fileHandles.set(uri, fHandle);
            this._mostRecentUri = uri;
        } else {
            throw new Error(`Failed to create file handle for ${uri}`);
        }
    }

    async read(uri: string): Promise<string> {
        uri = this.formatUri(uri);
        const fHandle = this._fileHandles.get(uri);
        if (fHandle) {
            const text = await (await fHandle.getFile()).text();
            this._mostRecentUri = uri;
            return text;
        } else {
            throw new Error(`Failed to read file handle for ${uri}`);
        }
    }
    async write(uri: string, data: any): Promise<void> {
        uri = this.formatUri(uri);
        let fHandle = this._fileHandles.get(uri);
        if (!fHandle) {
            await this.create(uri);
            fHandle = this._fileHandles.get(uri);
            this._mostRecentUri = uri;
        }
        if (fHandle) {
            const writable = await fHandle.createWritable();
            await writable.write(data);
            await writable.close();
            this._mostRecentUri = uri;
        } else {
            throw new Error(`Failed to write file handle for ${uri}`);
        }
    }
    async delete(uri: string): Promise<void> {
        uri = this.formatUri(uri);
        const fHandle = this._fileHandles.get(uri);
        if (fHandle) {
            // Find a way to delete the file content possibly using write
            this._fileHandles.delete(uri);
            this._symLinks.delete(uri);
            if (this._mostRecentUri === uri) {
                this._mostRecentUri = null;
            }
        } else {
            throw new Error(`Failed to delete file handle for ${uri}`);
        }
    }
    async onExternalFileChanges(subject: Subject<FileChangeEvent>) {
        // Due to limitations of the File System Access API, we cannot listen for all file changes.
        // Instead, we can use a polling mechanism to check for changes at the most recent uri
        let interval = 1000; // Check every second
        let recentChanges = { uri: '', data: '' };
        if (this._pollingInterval) {
            clearInterval(this._pollingInterval);
        }
        this._pollingInterval = Number(
            setInterval(async () => {
                try {
                    if (this._mostRecentUri) {
                        const content = await this.read(this._mostRecentUri);
                        if (recentChanges.uri !== this._mostRecentUri) {
                            recentChanges.uri = this._mostRecentUri;
                            recentChanges.data = content;
                        } else if (recentChanges.data !== content) {
                            recentChanges.data = content;
                            subject.next({
                                host: 'external',
                                type: 'update',
                                uri: this._mostRecentUri,
                                data: content,
                            });
                        }
                    }
                } catch (e) {
                    console.warn(
                        'Error reading file handle, possibly deleted or not found',
                        e
                    );
                }
            }, interval)
        );
    }
}
